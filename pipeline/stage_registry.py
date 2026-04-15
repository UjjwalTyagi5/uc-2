"""
pipeline.stage_registry
~~~~~~~~~~~~~~~~~~~~~~~~
Loads the pipeline_stages table from Azure SQL at startup and acts as the
single source of truth for all stage definitions.

Why this exists
---------------
Stage names and IDs are defined in the DB — not duplicated in Python enums.
At pipeline startup, StageRegistry:
    1. Fetches every row from pipeline_stages.
    2. Validates that each registered stage class has a NAME and STAGE_ID
       that match a real row — fail fast before any PR is processed.

This prevents code/DB drift: if someone adds a stage class with a typo, or
the DB table is updated but the code is not, the pipeline refuses to start.

Table expected
--------------
    [ras_procurement].[pipeline_stages]
    Columns: STAGE_ID (int), STAGE_NAME (varchar), STAGE_DESC (varchar),
             STAGE_DOMAIN (varchar), STAGE_SEQUENCE (int)
"""

from __future__ import annotations

from typing import Dict, List, Optional

import pyodbc
from loguru import logger

from pipeline.models import StageDefinition


class StageRegistry:
    """
    Runtime registry of all pipeline stages loaded from the DB.

    Parameters
    ----------
    conn_str:
        pyodbc connection string for the Azure SQL DB.

    Raises
    ------
    pyodbc.Error
        If the DB connection fails or the pipeline_stages table is missing.
    RuntimeError
        If the table is empty (no stages configured in DB).
    """

    _LOAD_SQL = """
        SELECT
            [STAGE_ID],
            [STAGE_NAME],
            [STAGE_DESC],
            [STAGE_DOMAIN],
            [STAGE_SEQUENCE]
        FROM [ras_procurement].[pipeline_stages]
        ORDER BY [STAGE_ID]
    """

    def __init__(self, conn_str: str) -> None:
        self._conn_str = conn_str
        self._log      = logger.bind(component="StageRegistry")
        self._by_name: Dict[str, StageDefinition] = {}
        self._by_id:   Dict[int, StageDefinition] = {}
        self._load()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def get_by_name(self, stage_name: str) -> Optional[StageDefinition]:
        """Returns the StageDefinition for the given STAGE_NAME, or None."""
        return self._by_name.get(stage_name)

    def get_by_id(self, stage_id: int) -> Optional[StageDefinition]:
        """Returns the StageDefinition for the given STAGE_ID, or None."""
        return self._by_id.get(stage_id)

    def all_stages(self) -> List[StageDefinition]:
        """Returns all loaded stage definitions ordered by STAGE_ID."""
        return sorted(self._by_id.values(), key=lambda s: s.stage_id)

    def validate_stages(self, stages: list) -> None:
        """
        Validates that every stage class in `stages` has a NAME and STAGE_ID
        that matches a row in the pipeline_stages table.

        Parameters
        ----------
        stages:
            List of BaseStage instances to validate.

        Raises
        ------
        ValueError
            If any stage's NAME or STAGE_ID does not match the DB table,
            or if a NAME/STAGE_ID mismatch is found between code and DB.
        """
        self._log.info(f"Validating {len(stages)} stage(s) against pipeline_stages table")
        errors: List[str] = []

        for stage in stages:
            defn = self._by_name.get(stage.NAME)

            if defn is None:
                errors.append(
                    f"  Stage class {type(stage).__name__!r}: "
                    f"NAME={stage.NAME!r} not found in pipeline_stages table. "
                    f"Known names: {sorted(self._by_name.keys())}"
                )
                continue

            if stage.STAGE_ID != defn.stage_id:
                errors.append(
                    f"  Stage class {type(stage).__name__!r}: "
                    f"STAGE_ID={stage.STAGE_ID} in code but "
                    f"STAGE_ID={defn.stage_id} in pipeline_stages table "
                    f"for STAGE_NAME={stage.NAME!r}."
                )
                continue

            self._log.debug(
                f"  OK  {stage.NAME!r} "
                f"(ID={defn.stage_id}, domain={defn.stage_domain}, "
                f"seq={defn.stage_sequence})"
            )

        if errors:
            msg = "Stage validation failed:\n" + "\n".join(errors)
            self._log.critical(msg)
            raise ValueError(msg)

        self._log.info("All stages validated successfully")

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _load(self) -> None:
        """Fetches all rows from pipeline_stages and populates lookup dicts."""
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(self._LOAD_SQL)
            rows = cursor.fetchall()
            cursor.close()

            if not rows:
                raise RuntimeError(
                    "pipeline_stages table is empty — no stages configured in DB."
                )

            for row in rows:
                defn = StageDefinition(
                    stage_id=int(row[0]),
                    stage_name=str(row[1]),
                    stage_desc=str(row[2]),
                    stage_domain=str(row[3]),
                    stage_sequence=int(row[4]),
                )
                self._by_name[defn.stage_name] = defn
                self._by_id[defn.stage_id]     = defn

            self._log.info(
                f"Loaded {len(self._by_name)} stage(s) from pipeline_stages: "
                f"{[s.stage_name for s in self.all_stages()]}"
            )

        except pyodbc.Error as exc:
            self._log.critical(
                f"Cannot load pipeline_stages table: {exc}. "
                f"Ensure the table exists in [ras_procurement].[pipeline_stages]."
            )
            raise

        finally:
            conn.close()

    def _connect(self) -> pyodbc.Connection:
        try:
            return pyodbc.connect(self._conn_str, autocommit=True, timeout=0)
        except pyodbc.Error as exc:
            self._log.critical(f"Cannot connect to Azure SQL DB: {exc}")
            raise
