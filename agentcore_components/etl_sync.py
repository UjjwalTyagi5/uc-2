from __future__ import annotations

import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any

import pyodbc

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
            name="parallel_workers",
            display_name="Workers per Table",
            value=4,
            info="Parallel threads that split and load each table's rows concurrently.",
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
        return pyodbc.connect(conn_str, timeout=30)

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

    # ── segment loader with retry ─────────────────────────────────────────────

    def _load_segment(
        self,
        src_cs: str,
        tgt_cs: str,
        staging: str,
        column_sql: str,
        placeholders: str,
        select_base: str,
        offset: int,
        fetch_n: int,
        counter: list,
        lock: threading.Lock,
    ):
        retries = max(1, int(self.max_retries or 3))
        ins_sql = (
            f"INSERT INTO {self._quote(staging)} "
            f"({column_sql}) VALUES ({placeholders})"
        )
        seg_sql = (
            f"{select_base} "
            f"ORDER BY (SELECT NULL) "
            f"OFFSET {offset} ROWS FETCH NEXT {fetch_n} ROWS ONLY"
        )
        chunk = max(1, int(self.batch_size or 5000))

        for attempt in range(retries + 1):
            src = None
            tgt = None
            rows_this_attempt = 0
            try:
                src = self._connect(src_cs)
                tgt = self._connect(tgt_cs)

                sc = src.cursor()
                sc.execute(seg_sql)
                tc = tgt.cursor()
                tc.fast_executemany = True

                batch = []
                for row in sc:
                    batch.append(row)
                    if len(batch) >= chunk:
                        tc.executemany(ins_sql, batch)
                        rows_this_attempt += len(batch)
                        batch = []
                if batch:
                    tc.executemany(ins_sql, batch)
                    rows_this_attempt += len(batch)

                # commit only after entire segment loaded — clean rollback on retry
                tgt.commit()
                with lock:
                    counter[0] += rows_this_attempt
                return  # success

            except Exception as exc:
                if tgt:
                    try:
                        tgt.rollback()
                    except Exception:
                        pass
                if attempt >= retries or not self._is_transient(exc):
                    raise
                delay = 2 ** attempt
                self.log(
                    f"Segment offset={offset} attempt {attempt+1}/{retries} "
                    f"failed ({exc!s:.80}), retrying in {delay}s…"
                )
                time.sleep(delay)
            finally:
                for conn in (src, tgt):
                    if conn:
                        try:
                            conn.close()
                        except Exception:
                            pass

    # ── per-table sync with retry ─────────────────────────────────────────────

    def _sync_table(
        self,
        src_cs: str,
        tgt_cs: str,
        etl_id: int,
        source_table: str,
        target_table: str,
        filters: dict,
        exclusions: dict,
    ) -> dict:
        staging  = self._staging_table(target_table)
        workers  = max(1, int(self.parallel_workers or 4))
        retries  = max(1, int(self.max_retries or 3))
        result   = {
            "etl_id": etl_id, "source": source_table,
            "target": target_table, "status": "failed",
            "rows": 0, "error": "",
        }

        for attempt in range(retries + 1):
            tgt = None
            try:
                filter_key = source_table.split(".")[-1].lower()
                where      = filters.get(filter_key, "")

                # ── row count + columns ───────────────────────────────
                src = self._connect(src_cs)
                sc  = src.cursor()
                count_sql = (
                    f"SELECT COUNT(*) FROM {self._quote(source_table)}"
                    + (f" WHERE {where}" if where else "")
                )
                sc.execute(count_sql)
                total = sc.fetchone()[0]

                sc.execute(f"SELECT TOP 0 * FROM {self._quote(source_table)}")
                excluded = {c.lower() for c in exclusions.get(filter_key, [])}
                cols     = [col[0] for col in sc.description if col[0].lower() not in excluded]
                src.close()

                if not cols:
                    raise ValueError(f"No columns found for {source_table}")

                column_sql   = ", ".join(self._quote(c) for c in cols)
                placeholders = ", ".join("?" for _ in cols)
                select_base  = (
                    f"SELECT {column_sql} FROM {self._quote(source_table)}"
                    + (f" WHERE {where}" if where else "")
                )

                # ── create fresh staging ──────────────────────────────
                tgt = self._connect(tgt_cs)
                tc  = tgt.cursor()
                self._drop_staging(tc, staging)
                tc.execute(
                    f"SELECT TOP 0 {column_sql} INTO {self._quote(staging)} "
                    f"FROM {self._quote(target_table)}"
                )
                tgt.commit()

                # ── parallel segment load ─────────────────────────────
                counter = [0]
                lock    = threading.Lock()
                segment = max(1, total // workers)

                self.log(
                    f"[{target_table}] {total:,} rows | "
                    f"{workers} workers | segment={segment:,} | attempt {attempt+1}"
                )

                with ThreadPoolExecutor(max_workers=workers) as pool:
                    futures = []
                    for i in range(workers):
                        off = i * segment
                        n   = segment if i < workers - 1 else total - off
                        if n > 0:
                            futures.append(pool.submit(
                                self._load_segment,
                                src_cs, tgt_cs, staging,
                                column_sql, placeholders, select_base,
                                off, n, counter, lock,
                            ))
                    for f in as_completed(futures):
                        f.result()

                # ── atomic swap ───────────────────────────────────────
                tc.execute(f"TRUNCATE TABLE {self._quote(target_table)}")
                tc.execute(
                    f"INSERT INTO {self._quote(target_table)} ({column_sql}) "
                    f"SELECT {column_sql} FROM {self._quote(staging)}"
                )
                self._drop_staging(tc, staging)
                self._write_status(tc, etl_id, "Success", counter[0])
                tgt.commit()

                result.update(status="success", rows=counter[0])
                self.log(f"[{target_table}] done — {counter[0]:,} rows")
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
                    self.log(f"[{target_table}] FAILED: {exc}")
                    return result
                delay = 2 ** attempt
                self.log(
                    f"[{target_table}] attempt {attempt+1}/{retries} failed "
                    f"({exc!s:.80}), retrying in {delay}s…"
                )
                time.sleep(delay)
            finally:
                if tgt:
                    try:
                        tgt.close()
                    except Exception:
                        pass

        return result

    # ── main entry ────────────────────────────────────────────────────────────

    def run_sync(self) -> Data:
        src_cfg = self._data_to_dict(self.source_connection)
        tgt_cfg = self._data_to_dict(self.target_connection)
        src_cs  = self._sqlserver_conn_str(src_cfg)
        tgt_cs  = self._sqlserver_conn_str(tgt_cfg)
        filters    = json.loads(self.table_filters_json     or "{}")
        exclusions = json.loads(self.column_exclusions_json or "{}")

        # ── read control table ─────────────────────────────────────────
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
            return Data(data={"success": False, "error": f"Cannot read control table: {exc}"})

        if not configs:
            return Data(data={
                "success": True,
                "message": "No tables enabled for sync.",
                "tables_synced": 0,
            })

        table_workers = max(1, int(self.max_table_workers or 4))
        per_table     = max(1, int(self.parallel_workers   or 4))
        retries       = max(1, int(self.max_retries        or 3))
        self.log(
            f"Syncing {len(configs)} table(s) | "
            f"{table_workers} parallel tables | "
            f"{per_table} workers/table | "
            f"{retries} retries on transient errors"
        )

        # ── disable FK constraints ─────────────────────────────────────
        if self.disable_constraints:
            try:
                fk = self._connect(tgt_cs)
                fc = fk.cursor()
                for _, _, tgt_tbl, _ in configs:
                    self._toggle_constraints(fc, tgt_tbl, enable=False)
                fk.commit()
                fk.close()
            except Exception as exc:
                self.log(f"Warning: FK disable failed: {exc}")

        # ── sync all tables in parallel ────────────────────────────────
        results = []
        with ThreadPoolExecutor(max_workers=table_workers) as pool:
            futures = {
                pool.submit(
                    self._sync_table,
                    src_cs, tgt_cs,
                    etl_id, src_tbl, tgt_tbl,
                    filters, exclusions,
                ): tgt_tbl
                for etl_id, src_tbl, tgt_tbl, _ in configs
            }
            for f in as_completed(futures):
                results.append(f.result())

        # ── re-enable FK constraints ───────────────────────────────────
        if self.disable_constraints:
            try:
                fk2 = self._connect(tgt_cs)
                fc2 = fk2.cursor()
                for _, _, tgt_tbl, _ in configs:
                    self._toggle_constraints(fc2, tgt_tbl, enable=True)
                fk2.commit()
                fk2.close()
            except Exception as exc:
                self.log(f"Warning: FK re-enable failed: {exc}")

        failed = [r for r in results if r["status"] == "failed"]
        output = {
            "success":       len(failed) == 0,
            "tables_synced": len(results),
            "failed_tables": len(failed),
            "results":       results,
        }
        self.status = output
        return Data(data=output)
