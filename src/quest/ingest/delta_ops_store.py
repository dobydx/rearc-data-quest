"""``OpsStore`` on the Delta tables in ``quest_ops``; every write is an idempotent MERGE."""

import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from quest.contracts.loader import silver_contracts, silver_table_of
from quest.ingest.models import FileState, FileStatus, FileVersion, IngestRun, RunFile
from quest.ingest.planner import PipelineUpdateInfo
from quest.ops_ddl import OPS_TABLE_DDL, OPS_TABLES, SCHEMA, check_statements

if TYPE_CHECKING:
    from _typeshed import DataclassInstance
    from pyspark.sql import SparkSession

EVENT_LOG_TABLE = f"{SCHEMA}.pipeline_event_log"
# The pipeline stamps its code version on this gold table (TBLPROPERTY quest.code_version).
CODE_VERSION_TABLE = "quest_gold.series_best_year"
CODE_VERSION_PROPERTY = "quest.code_version"
_CATALOG_NAME = re.compile(r"^[A-Za-z0-9_]+$")


def latest_real_update_state(spark: "SparkSession", event_log: str) -> str | None:
    """Return the latest state of the newest update that was not a dry run, or None.

    Validate-only updates also log COMPLETED, so they are left out. An update still in flight
    returns its current state (RUNNING, ...), which the gate treats as not ok.
    """
    # get_json_object rather than the details:path syntax, so the query also runs locally.
    row = spark.sql(
        f"""
        WITH real_update AS (
          SELECT origin.update_id AS update_id
          FROM {event_log}
          WHERE event_type = 'create_update'
            AND coalesce(get_json_object(details, '$.create_update.validate_only'), 'false')
                <> 'true'
          ORDER BY timestamp DESC
          LIMIT 1
        )
        SELECT get_json_object(e.details, '$.update_progress.state') AS state
        FROM {event_log} AS e
        JOIN real_update AS r ON e.origin.update_id = r.update_id
        WHERE e.event_type = 'update_progress'
        ORDER BY e.timestamp DESC
        LIMIT 1
        """  # noqa: S608 - the table name is a module constant or a test view
    ).first()
    return None if row is None else str(row["state"])


def catalog_identifier(catalog: str) -> str:
    """Return ``catalog`` quoted for SQL, after checking it is a plain identifier.

    Raises:
        ValueError: If the name has anything but letters, digits and underscores.
    """
    if not _CATALOG_NAME.match(catalog):
        raise ValueError(f"catalog name {catalog!r} is not a plain identifier")
    return f"`{catalog}`"


def row_values(record: "DataclassInstance", columns: Sequence[str]) -> tuple[Any, ...]:
    """Return a dataclass record's values in table column order, enums as plain strings.

    Raises:
        KeyError: If a table column has no field on the record.
    """
    values = asdict(record)
    return tuple(_plain(values[column]) for column in columns)


def file_state_from_row(row: Mapping[str, Any]) -> FileState:
    """Build a ``FileState`` from a Spark row dict, restoring the enum and UTC times."""
    return FileState(
        **{
            **row,
            "status": FileStatus(row["status"]),
            "http_last_modified": _as_utc(row["http_last_modified"]),
            "updated_at": _as_utc(row["updated_at"]),
        }
    )


class DeltaOpsStore:
    """Ops tables on Delta. Needs Databricks: MERGE is not in open-source Spark."""

    def __init__(self, spark: "SparkSession", catalog: str) -> None:
        """Select the catalog so every statement can use two-part names."""
        self._spark = spark
        spark.sql(f"USE CATALOG {catalog_identifier(catalog)}")

    def ensure_tables(self) -> None:
        """Create the ops tables and add any CHECK constraint that is missing or changed."""
        for name, table in OPS_TABLES.items():
            self._spark.sql(OPS_TABLE_DDL[name])
            properties = {
                row["key"]: row["value"]
                for row in self._spark.sql(f"SHOW TBLPROPERTIES {SCHEMA}.{name}").collect()
            }
            for statement in check_statements(table, properties):
                self._spark.sql(statement)

    def start_run(self, run: IngestRun) -> None:
        """Upsert the run row."""
        self.merge("ingest_runs", [run])

    def finish_run(self, run: IngestRun) -> None:
        """Upsert the run row with its final status and counts."""
        self.merge("ingest_runs", [run])

    def file_states(self, source: str) -> dict[str, FileState]:
        """Return the states of ``source`` keyed by file name."""
        rows = self._spark.sql(
            f"SELECT * FROM {SCHEMA}.file_state WHERE source = :source",  # noqa: S608
            args={"source": source},
        ).collect()
        return {row["file_name"]: file_state_from_row(row.asDict()) for row in rows}

    def known_shas(self, source: str, file_name: str) -> set[str]:
        """Return every SHA-256 recorded for one file."""
        rows = self._spark.sql(
            f"SELECT sha256 FROM {SCHEMA}.file_versions "  # noqa: S608
            "WHERE source = :source AND file_name = :file_name",
            args={"source": source, "file_name": file_name},
        ).collect()
        return {row["sha256"] for row in rows}

    def served_shas(self, source: str, file_name: str) -> set[str]:
        """Return every SHA-256 a committed pointer flip made current for one file."""
        rows = self._spark.sql(
            f"SELECT DISTINCT sha256_after FROM {SCHEMA}.ingest_run_files "  # noqa: S608
            "WHERE source = :source AND file_name = :file_name AND pointer_flipped "
            "AND sha256_after IS NOT NULL",
            args={"source": source, "file_name": file_name},
        ).collect()
        return {row["sha256_after"] for row in rows}

    def insert_version(self, version: FileVersion) -> None:
        """Insert a version unless its key exists; versions are immutable."""
        self.merge("file_versions", [version], update=False)

    def record_run_files(self, rows: Sequence[RunFile]) -> None:
        """Upsert the run's file rows."""
        self.merge("ingest_run_files", rows)

    def commit_states(self, states: Sequence[FileState]) -> None:
        """Upsert every state in one MERGE, which is one Delta transaction."""
        self.merge("file_state", states)

    def silver_lineage(self) -> set[tuple[str, str]] | None:
        """Return served ``(file_name, sha256)`` pairs, or None if a silver table is absent."""
        tables = [
            f"quest_silver.{silver_table_of(contract)}{suffix}"
            for contract in silver_contracts()
            for suffix in ("", "_quarantine")
        ]
        if not all(self._spark.catalog.tableExists(table) for table in tables):
            return None
        # Table names come from the packaged contracts, never from user input.
        query = " UNION ".join(
            f"SELECT DISTINCT _file_name, _sha256 FROM {t}"  # noqa: S608
            for t in tables
        )
        return {(row[0], row[1]) for row in self._spark.sql(query).collect()}

    def last_pipeline_update(self) -> PipelineUpdateInfo | None:
        """Return the latest update state from the event log and the code version gold carries."""
        if not all(
            self._spark.catalog.tableExists(t) for t in (EVENT_LOG_TABLE, CODE_VERSION_TABLE)
        ):
            return None
        state = latest_real_update_state(self._spark, EVENT_LOG_TABLE)
        if state is None:
            return None
        properties = {
            row["key"]: row["value"]
            for row in self._spark.sql(f"SHOW TBLPROPERTIES {CODE_VERSION_TABLE}").collect()
        }
        return PipelineUpdateInfo(state=state, code_version=properties.get(CODE_VERSION_PROPERTY))

    def merge(
        self, table: str, records: Sequence["DataclassInstance"], update: bool = True
    ) -> None:
        """MERGE records into an ops table on its primary key; ``update=False`` only inserts."""
        if not records:
            return
        target = f"{SCHEMA}.{table}"
        schema = self._spark.table(target).schema
        rows = [row_values(record, schema.fieldNames()) for record in records]
        view = f"_quest_ops_{table}_source"
        self._spark.createDataFrame(rows, schema).createOrReplaceTempView(view)
        on = " AND ".join(f"t.{key} = s.{key}" for key in OPS_TABLES[table].key)
        when_matched = "WHEN MATCHED THEN UPDATE SET * " if update else ""
        self._spark.sql(
            f"MERGE INTO {target} AS t USING {view} AS s ON {on} "
            f"{when_matched}WHEN NOT MATCHED THEN INSERT *"
        )


def _plain(value: object) -> object:
    return str(value) if isinstance(value, StrEnum) else value


def _as_utc(value: datetime | None) -> datetime | None:
    # Spark returns TIMESTAMP as a naive datetime. get_spark pins the session time zone to UTC
    # and Databricks drivers run in UTC, so a naive value here is UTC.
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=UTC)
