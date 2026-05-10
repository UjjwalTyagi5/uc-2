from __future__ import annotations

import json
import time
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any

import pyodbc
from loguru import logger

from agentcore.custom import Node
from agentcore.io import BoolInput, DataInput, IntInput, MessageInput, MessageTextInput, MultilineInput, Output
from agentcore.schema.data import Data


class AgentCoreETLSync(Node):
    display_name = "AgentCore ETL Sync"
    description = "Syncs enabled tables from source SQL Server to target SQL Server with parallel processing."
    icon = "database"
    name = "AgentCoreETLSync"

    inputs = [
        MessageInput(
            name="trigger",
            display_name="Trigger",
            info="Connect Control Panel / Schedule / Trigger output here.",
            advanced=True,
        ),
        DataInput(
            name="source_connection",
            display_name="Source DB Connection",
            info="Connect On-Prem Database Connector: Connection Config.",
        ),
        DataInput(
            name="target_connection",
            display_name="Target DB Connection",
            info="Connect Azure SQL Database Connector: Connection Config.",
        ),
        MessageTextInput(
            name="sync_control_table",
            display_name="Sync Control Table",
            value="ras_procurement.etl_sync_control",
        ),
        MessageTextInput(
            name="sync_status_table",
            display_name="Sync Status Table",
            value="ras_procurement.etl_sync_status",
        ),
        IntInput(
            name="batch_size",
            display_name="Batch Size (rows per commit)",
            value=5000,
            info="Number of rows committed per INSERT batch.",
        ),
        IntInput(
            name="max_table_workers",
            display_name="Max Parallel Tables",
            value=4,
            info="How many tables to sync at the same time. Set to 1 for sequential.",
        ),
        IntInput(
            name="max_retries",
            display_name="Max Retries (transient errors)",
            value=3,
            info="How many times to retry a segment or table on network/timeout errors.",
        ),
        MultilineInput(
            name="table_filters_json",
            display_name="Table Filters JSON",
            value="""{"purchase_req_mst": "C_DATETIME > '2020-01-01'", "purchase_req_detail": "C_DATETIME > '2020-01-01'", "purchase_attachments": "UPLOADED_ON > '2020-01-01'"}""",
            advanced=True,
        ),
        MultilineInput(
            name="column_exclusions_json",
            display_name="Column Exclusions JSON",
            value="""{"purchase_req_mst": ["priority_id", "priority_reason", "copy_req", "copy_req_id"]}""",
            advanced=True,
        ),
        BoolInput(
            name="disable_constraints",
            display_name="Disable Target FK Constraints",
            value=True,
            advanced=True,
        ),
    ]

    outputs = [
        Output(display_name="Sync Result", name="result", method="run_sync"),
    ]

    # ── helpers ───────────────────────────────────────────────────────────────

    def _data_to_dict(self, value: Any) -> dict:
        if isinstance(value, dict):
            return value
        if hasattr(value, "data") and isinstance(value.data, dict):
            return value.data
        if hasattr(value, "value") and isinstance(value.value, dict):
            return value.value
        raise ValueError(f"Invalid connector data: {type(value)}")

    def _sqlserver_conn_str(self, cfg: dict) -> str:
        drivers = pyodbc.drivers()
        selected = next(
            (d for d in [
                "ODBC Driver 18 for SQL Server",
                "ODBC Driver 17 for SQL Server",
                "SQL Server Native Client 11.0",
                "SQL Server",
            ] if d in drivers),
            None,
        )
        if not selected:
            raise ValueError(f"No SQL Server ODBC driver found. Installed: {drivers}")
        encrypt = "yes" if cfg.get("ssl_enabled") else "no"
        return (
            f"DRIVER={{{selected}}};"
            f"SERVER={cfg['host']},{cfg.get('port', 1433)};"
            f"DATABASE={cfg['database_name']};"
            f"UID={cfg['username']};"
            f"PWD={cfg['password']};"
            f"Encrypt={encrypt};"
            "TrustServerCertificate=yes;"
            "Connection Timeout=30;"
        )

    def _connect(self, conn_str: str) -> pyodbc.Connection:
        conn = pyodbc.connect(conn_str, timeout=30)
        conn.timeout = 600  # query execution timeout: 10 min max per statement
        return conn

    def _is_transient(self, exc: Exception) -> bool:
        transient_states = {"08S01", "08001", "HYT00", "HYT01", "40001", "40613"}
        transient_words  = [
            "communication link failure", "tcp provider",
            "connection timeout", "connection reset",
            "timed out", "broken pipe", "10060", "10054",
        ]
        for arg in getattr(exc, "args", []):
            if isinstance(arg, str):
                arg_lower = arg.lower()
                if any(s in arg for s in transient_states):
                    return True
                if any(w in arg_lower for w in transient_words):
                    return True
            if hasattr(arg, "SQLSTATE") and arg.SQLSTATE in transient_states:
                return True
        return any(w in str(exc).lower() for w in transient_words)

    def _quote(self, name: str) -> str:
        parts = [p.strip().strip("[]") for p in str(name).split(".")]
        return ".".join(f"[{p}]" for p in parts if p)

    def _staging_table(self, target_table: str) -> str:
        if "." in target_table:
            schema, table = target_table.rsplit(".", 1)
            return f"{schema}.{table}_staging"
        return f"{target_table}_staging"

    def _drop_staging(self, cursor, staging: str):
        safe = staging.replace("'", "''")
        cursor.execute(
            f"IF OBJECT_ID(N'{safe}', N'U') IS NOT NULL DROP TABLE {self._quote(staging)}"
        )

    def _toggle_constraints(self, cursor, table: str, enable: bool):
        action = "WITH CHECK CHECK CONSTRAINT ALL" if enable else "NOCHECK CONSTRAINT ALL"
        cursor.execute(f"ALTER TABLE {self._quote(table)} {action}")

    def _write_status(self, cursor, etl_id, status, row_count, error=None):
        cursor.execute(
            f"INSERT INTO {self._quote(self.sync_status_table)} "
            "(ETLId, LastSyncTime, SyncStatus, NumberOfRecordFetched, ErrorMessage) "
            "VALUES (?, ?, ?, ?, ?)",
            etl_id, datetime.now(), status, row_count, error,
        )

    # ── per-table sync with retry ─────────────────────────────────────────────
    # Runs inside ThreadPoolExecutor — must use logger.* not self.log()

    def _sync_table(
        self,
        src_cs: str,
        tgt_cs: str,
        etl_id: int,
        source_table: str,
        target_table: str,
        filters: dict,
        exclusions: dict,
        job_id: str,
    ) -> dict:
        staging = self._staging_table(target_table)
        retries = max(1, int(self.max_retries or 3))
        chunk   = max(1, int(self.batch_size or 5000))
        result  = {
            "etl_id": etl_id, "source": source_table,
            "target": target_table, "status": "failed",
            "rows": 0, "error": "",
        }
        tag = f"[job={job_id}][{target_table}]"

        logger.info(f"{tag} ── table sync started (source: {source_table})")

        for attempt in range(retries + 1):
            src = None
            tgt = None
            try:
                filter_key = source_table.split(".")[-1].lower()
                where      = filters.get(filter_key, "")

                # ── schema from source ────────────────────────────────
                logger.info(f"{tag} reading schema from source")
                src = self._connect(src_cs)
                sc  = src.cursor()
                sc.execute(f"SELECT TOP 0 * FROM {self._quote(source_table)}")
                excluded = {c.lower() for c in exclusions.get(filter_key, [])}
                cols     = [col[0] for col in sc.description if col[0].lower() not in excluded]

                if not cols:
                    raise ValueError(f"No columns found for {source_table}")

                column_sql   = ", ".join(self._quote(c) for c in cols)
                placeholders = ", ".join("?" for _ in cols)
                select_sql   = (
                    f"SELECT {column_sql} FROM {self._quote(source_table)}"
                    + (f" WHERE {where}" if where else "")
                )

                logger.info(
                    f"{tag} {len(cols)} columns"
                    + (f" | filter: {where}" if where else "")
                    + (f" | excluding: {list(excluded)}" if excluded else "")
                )

                # ── create fresh staging ──────────────────────────────
                logger.info(f"{tag} creating staging table: {staging}")
                tgt = self._connect(tgt_cs)
                tc  = tgt.cursor()
                self._drop_staging(tc, staging)
                tc.execute(
                    f"SELECT TOP 0 {column_sql} INTO {self._quote(staging)} "
                    f"FROM {self._quote(target_table)}"
                )
                tgt.commit()
                logger.info(f"{tag} staging table ready — streaming rows (batch={chunk:,})")

                # ── single-pass streaming load ────────────────────────
                # One source scan, batched inserts — no OFFSET redundancy.
                ins_sql = (
                    f"INSERT INTO {self._quote(staging)} "
                    f"({column_sql}) VALUES ({placeholders})"
                )
                tc.fast_executemany = True
                sc.execute(select_sql)
                total_rows = 0
                batch = []
                for row in sc:
                    batch.append(row)
                    if len(batch) >= chunk:
                        tc.executemany(ins_sql, batch)
                        total_rows += len(batch)
                        tgt.commit()
                        logger.info(f"{tag} {total_rows:,} rows loaded so far…")
                        batch = []
                if batch:
                    tc.executemany(ins_sql, batch)
                    total_rows += len(batch)
                    tgt.commit()

                logger.info(f"{tag} {total_rows:,} rows loaded into staging")

                # ── atomic swap ───────────────────────────────────────
                logger.info(f"{tag} swapping staging → target (TRUNCATE + INSERT)")
                tc.execute(f"TRUNCATE TABLE {self._quote(target_table)}")
                tc.execute(
                    f"INSERT INTO {self._quote(target_table)} ({column_sql}) "
                    f"SELECT {column_sql} FROM {self._quote(staging)}"
                )
                self._drop_staging(tc, staging)
                self._write_status(tc, etl_id, "Success", counter[0])
                tgt.commit()

                result.update(status="success", rows=total_rows)
                logger.info(f"{tag} ✓ DONE — {total_rows:,} rows synced successfully")
                return result

            except Exception as exc:
                if tgt:
                    try:
                        tgt.rollback()
                        self._drop_staging(tgt.cursor(), staging)
                        tgt.commit()
                    except Exception:
                        pass
                if attempt >= retries or not self._is_transient(exc):
                    try:
                        conn2 = self._connect(tgt_cs)
                        cur2  = conn2.cursor()
                        self._write_status(cur2, etl_id, "Failed", result["rows"], str(exc))
                        conn2.commit()
                        conn2.close()
                    except Exception:
                        pass
                    result["error"] = str(exc)
                    logger.error(f"{tag} ✗ FAILED (attempt {attempt+1}): {exc}")
                    return result
                delay = 2 ** attempt
                logger.warning(
                    f"{tag} attempt {attempt+1}/{retries} failed ({exc!s:.80}) "
                    f"— retrying in {delay}s"
                )
                time.sleep(delay)
            finally:
                for conn in (src, tgt):
                    if conn:
                        try:
                            conn.close()
                        except Exception:
                            pass

        return result

    # ── main entry ────────────────────────────────────────────────────────────

    def run_sync(self) -> Data:
        logger.info("[ETL] run_sync() triggered — validating connections")

        # Build and smoke-test connections before touching anything
        try:
            src_cfg = self._data_to_dict(self.source_connection)
            tgt_cfg = self._data_to_dict(self.target_connection)
            src_cs  = self._sqlserver_conn_str(src_cfg)
            tgt_cs  = self._sqlserver_conn_str(tgt_cfg)
        except Exception as exc:
            logger.error(f"[ETL] connection config error: {exc}")
            return Data(data={"success": False, "error": f"Connection config error: {exc}"})

        for label, cs in [("source", src_cs), ("target", tgt_cs)]:
            try:
                c = self._connect(cs)
                c.close()
                logger.info(f"[ETL] {label} DB connection OK")
            except Exception as exc:
                logger.error(f"[ETL] cannot connect to {label} DB: {exc}")
                return Data(data={"success": False, "error": f"Cannot connect to {label} DB: {exc}"})

        filters    = json.loads(self.table_filters_json     or "{}")
        exclusions = json.loads(self.column_exclusions_json or "{}")

        # ── read control table ─────────────────────────────────────────
        logger.info(f"[ETL] reading control table: {self.sync_control_table}")
        try:
            ctrl = self._connect(tgt_cs)
            cur  = ctrl.cursor()
            cur.execute(
                f"SELECT ETLId, Source, Destination, SyncHours "
                f"FROM {self._quote(self.sync_control_table)} "
                f"WHERE EnableSync = 1 ORDER BY ETLId"
            )
            configs = cur.fetchall()
            ctrl.close()
        except Exception as exc:
            logger.error(f"[ETL] cannot read control table: {exc}")
            return Data(data={"success": False, "error": f"Cannot read control table: {exc}"})

        if not configs:
            logger.info("[ETL] no tables enabled for sync — nothing to do")
            return Data(data={
                "success": True,
                "message": "No tables enabled for sync.",
                "tables_synced": 0,
            })

        table_workers = max(1, int(self.max_table_workers or 4))
        retries       = max(1, int(self.max_retries        or 3))

        logger.info(
            f"[ETL] {len(configs)} table(s) enabled: "
            f"{[row[2] for row in configs]}"
        )
        logger.info(
            f"[ETL] config — table_workers={table_workers} | "
            f"max_retries={retries} | batch_size={self.batch_size}"
        )

        # Fire-and-forget: return immediately so AKS proxy timeout is never hit.
        # The sync runs in a daemon thread; track progress via sync_status_table.
        job_id = str(uuid.uuid4())[:8]
        logger.info(f"[ETL] launching background sync | job={job_id}")

        threading.Thread(
            target=_run_full_sync,
            args=(self, src_cs, tgt_cs, filters, exclusions, configs,
                  table_workers, retries, job_id),
            daemon=True,
            name=f"etl-{job_id}",
        ).start()

        logger.info(f"[ETL] background thread started | job={job_id} — returning to caller")
        self.log(
            f"ETL sync started | job={job_id} | "
            f"{len(configs)} table(s): {[row[2] for row in configs]}"
        )
        return Data(data={
            "success": True,
            "job_id": job_id,
            "tables_queued": len(configs),
            "tables": [row[2] for row in configs],
            "message": "Sync running in background. Check sync_status_table for per-table results.",
        })


def _run_full_sync(node, src_cs, tgt_cs, filters, exclusions, configs,
                   table_workers, retries, job_id):
    """Background daemon thread — uses logger.* only, never self.log()."""
    started_at = datetime.now()
    logger.info(
        f"[ETL job={job_id}] ══ background sync started at {started_at:%Y-%m-%d %H:%M:%S} ══"
    )
    logger.info(
        f"[ETL job={job_id}] tables to sync ({len(configs)}): "
        f"{[row[2] for row in configs]}"
    )

    # ── disable FK constraints ─────────────────────────────────────────
    if node.disable_constraints:
        logger.info(f"[ETL job={job_id}] disabling FK constraints on all target tables")
        try:
            fk = node._connect(tgt_cs)
            fc = fk.cursor()
            for _, _, tgt_tbl, _ in configs:
                node._toggle_constraints(fc, tgt_tbl, enable=False)
                logger.info(f"[ETL job={job_id}] FK disabled: {tgt_tbl}")
            fk.commit()
            fk.close()
        except Exception as exc:
            logger.warning(f"[ETL job={job_id}] FK disable failed (continuing): {exc}")

    # ── sync all tables in parallel ────────────────────────────────────
    logger.info(
        f"[ETL job={job_id}] starting parallel sync "
        f"({table_workers} tables at a time)"
    )
    results = []
    with ThreadPoolExecutor(max_workers=table_workers) as pool:
        futures = {
            pool.submit(
                node._sync_table,
                src_cs, tgt_cs,
                etl_id, src_tbl, tgt_tbl,
                filters, exclusions, job_id,
            ): tgt_tbl
            for etl_id, src_tbl, tgt_tbl, _ in configs
        }
        for f in as_completed(futures):
            result = f.result()
            results.append(result)
            status_icon = "✓" if result["status"] == "success" else "✗"
            logger.info(
                f"[ETL job={job_id}] {status_icon} table completed: "
                f"{result['target']} — status={result['status']} "
                f"rows={result['rows']:,}"
                + (f" error={result['error']}" if result["error"] else "")
            )

    # ── re-enable FK constraints ───────────────────────────────────────
    if node.disable_constraints:
        logger.info(f"[ETL job={job_id}] re-enabling FK constraints")
        try:
            fk2 = node._connect(tgt_cs)
            fc2 = fk2.cursor()
            for _, _, tgt_tbl, _ in configs:
                node._toggle_constraints(fc2, tgt_tbl, enable=True)
                logger.info(f"[ETL job={job_id}] FK re-enabled: {tgt_tbl}")
            fk2.commit()
            fk2.close()
        except Exception as exc:
            logger.warning(f"[ETL job={job_id}] FK re-enable failed: {exc}")

    # ── final summary ──────────────────────────────────────────────────
    elapsed   = (datetime.now() - started_at).total_seconds()
    succeeded = [r for r in results if r["status"] == "success"]
    failed    = [r for r in results if r["status"] != "success"]
    total_rows = sum(r["rows"] for r in results)

    logger.info(
        f"[ETL job={job_id}] ══ background sync finished in {elapsed:.1f}s ══"
    )
    logger.info(
        f"[ETL job={job_id}] summary: "
        f"{len(succeeded)}/{len(results)} tables succeeded | "
        f"{total_rows:,} total rows synced"
    )
    if failed:
        logger.error(
            f"[ETL job={job_id}] FAILED tables ({len(failed)}): "
            f"{[r['target'] for r in failed]}"
        )
        for r in failed:
            logger.error(
                f"[ETL job={job_id}]   ✗ {r['target']}: {r['error']}"
            )
