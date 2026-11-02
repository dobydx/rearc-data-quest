"""DDL for the plain Delta tables in ``quest_ops`` that the ingest and validate tasks write."""

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum

from quest.ingest.models import FileStatus, RunFileAction, RunStatus
from quest.sql import sql_string

SCHEMA = "quest_ops"
CONSTRAINT_PROPERTY_PREFIX = "delta.constraints."


@dataclass(frozen=True)
class OpsColumn:
    """One column: name, Spark SQL type, nullability."""

    name: str
    sql_type: str
    nullable: bool = False


@dataclass(frozen=True)
class OpsCheck:
    """One enforced CHECK constraint."""

    name: str
    condition: str


@dataclass(frozen=True)
class OpsTable:
    """One ops table: columns, informational primary key, CHECK constraints, comment."""

    name: str
    comment: str
    columns: tuple[OpsColumn, ...]
    key: tuple[str, ...]
    checks: tuple[OpsCheck, ...] = ()


def _one_of(column: str, values: tuple[str, ...] | type[StrEnum]) -> str:
    options = ", ".join(sql_string(str(value)) for value in values)
    return f"{column} IN ({options})"


def _col(name: str, sql_type: str, nullable: bool = False) -> OpsColumn:
    return OpsColumn(name, sql_type, nullable)


OPS_TABLES: Mapping[str, OpsTable] = {
    t.name: t
    for t in (
        OpsTable(
            name="ingest_runs",
            comment="One row per ingest task run with its counts; the per-run audit.",
            columns=(
                _col("run_id", "STRING"),
                _col("job_id", "STRING", nullable=True),
                _col("job_run_id", "STRING", nullable=True),
                _col("started_at", "TIMESTAMP"),
                _col("ended_at", "TIMESTAMP", nullable=True),
                _col("status", "STRING"),
                _col("code_version", "STRING"),
                _col("trigger", "STRING"),
                _col("files_listed", "INT"),
                _col("files_skipped_fingerprint", "INT"),
                _col("files_not_modified", "INT"),
                _col("files_same_hash", "INT"),
                _col("versions_landed", "INT"),
                _col("pointers_flipped", "INT"),
                _col("files_missing", "INT"),
                _col("files_deleted", "INT"),
                _col("files_self_healed", "INT"),
                _col("files_failed", "INT"),
                _col("error_class", "STRING", nullable=True),
                _col("error_message", "STRING", nullable=True),
                _col("params_json", "STRING"),
            ),
            key=("run_id",),
            checks=(OpsCheck("ingest_runs_status", _one_of("status", RunStatus)),),
        ),
        OpsTable(
            name="ingest_run_files",
            comment="One row per run and file: what the run did with it.",
            columns=(
                _col("run_id", "STRING"),
                _col("source", "STRING"),
                _col("file_name", "STRING"),
                _col("action", "STRING"),
                _col("sha256_before", "STRING", nullable=True),
                _col("sha256_after", "STRING", nullable=True),
                _col("pointer_flipped", "BOOLEAN"),
                _col("http_status", "INT", nullable=True),
                _col("bytes", "BIGINT", nullable=True),
                _col("elapsed_ms", "BIGINT"),
                _col("error", "STRING", nullable=True),
                _col("recorded_at", "TIMESTAMP"),
            ),
            key=("run_id", "source", "file_name"),
            checks=(OpsCheck("ingest_run_files_action", _one_of("action", RunFileAction)),),
        ),
        OpsTable(
            name="file_versions",
            comment="One row per distinct landed blob; insert-only.",
            columns=(
                _col("source", "STRING"),
                _col("file_name", "STRING"),
                _col("sha256", "STRING"),
                _col("file_role", "STRING", nullable=True),
                _col("landing_path", "STRING"),
                _col("size_bytes", "BIGINT"),
                _col("line_count", "BIGINT"),
                _col("http_last_modified", "TIMESTAMP", nullable=True),
                _col("request_url", "STRING"),
                _col("content_type", "STRING"),
                _col("header_ok", "BOOLEAN"),
                _col("header_observed", "STRING"),
                _col("header_diff", "STRING", nullable=True),
                _col("first_seen_run_id", "STRING"),
                _col("first_seen_at", "TIMESTAMP"),
            ),
            key=("source", "file_name", "sha256"),
        ),
        OpsTable(
            name="file_state",
            comment="Current pointer per logical file; silver serves these versions.",
            columns=(
                _col("source", "STRING"),
                _col("file_name", "STRING"),
                _col("current_sha256", "STRING"),
                _col("status", "STRING"),
                _col("consecutive_missing", "INT"),
                _col("listing_size", "BIGINT", nullable=True),
                _col("listing_ts_raw", "STRING", nullable=True),
                _col("http_last_modified", "TIMESTAMP", nullable=True),
                _col("last_checked_run_id", "STRING"),
                _col("last_changed_run_id", "STRING"),
                _col("missing_since_run_id", "STRING", nullable=True),
                _col("updated_at", "TIMESTAMP"),
            ),
            key=("source", "file_name"),
            checks=(OpsCheck("file_state_status", _one_of("status", FileStatus)),),
        ),
        OpsTable(
            name="table_versions",
            comment="One row per validate run and data table: Delta version, rows, checksum.",
            columns=(
                _col("run_id", "STRING"),
                _col("table_name", "STRING"),
                _col("layer", "STRING"),
                # NULL for a materialized view: it has no readable Delta history.
                _col("delta_version", "BIGINT", nullable=True),
                _col("row_count", "BIGINT"),
                # A sum of 64-bit hashes overflows BIGINT, and ANSI mode raises on overflow.
                _col("content_checksum", "DECIMAL(38,0)"),
                _col("changed_since_previous", "BOOLEAN"),
                _col("captured_at", "TIMESTAMP"),
            ),
            key=("run_id", "table_name"),
        ),
        OpsTable(
            name="dq_checks",
            comment="One row per validate run, check and table.",
            columns=(
                _col("run_id", "STRING"),
                _col("check_code", "STRING"),
                _col("table_name", "STRING"),
                _col("layer", "STRING"),
                _col("severity", "STRING"),
                _col("status", "STRING"),
                _col("observed_value", "STRING"),
                _col("threshold", "STRING", nullable=True),
                _col("failing_rows", "BIGINT", nullable=True),
                _col("details", "STRING", nullable=True),
                _col("checked_at", "TIMESTAMP"),
            ),
            key=("run_id", "check_code", "table_name"),
            checks=(
                OpsCheck("dq_checks_severity", _one_of("severity", ("warn", "fail"))),
                OpsCheck("dq_checks_status", _one_of("status", ("pass", "warn", "fail", "error"))),
            ),
        ),
    )
}


def _create_table(table: OpsTable) -> str:
    columns = ",\n".join(
        f"  {c.name} {c.sql_type}{'' if c.nullable else ' NOT NULL'}" for c in table.columns
    )
    key = ", ".join(table.key)
    return (
        f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{table.name} (\n{columns},\n"
        f"  CONSTRAINT {table.name}_pk PRIMARY KEY ({key})\n) COMMENT {sql_string(table.comment)}"
    )


def check_statements(table: OpsTable, existing_properties: Mapping[str, str]) -> list[str]:
    """Statements that bring a table's CHECK constraints in line with ``OPS_TABLES``.

    Delta records each CHECK as the table property ``delta.constraints.<name>``; a constraint
    already there with the same expression is left alone, so a rerun writes no new version.
    ADD CONSTRAINT has no IF NOT EXISTS, and CHECK is not allowed in CREATE TABLE.
    """
    statements = []
    prefix = f"ALTER TABLE {SCHEMA}.{table.name}"
    for check in table.checks:
        if existing_properties.get(f"{CONSTRAINT_PROPERTY_PREFIX}{check.name}") == check.condition:
            continue
        statements.append(f"{prefix} DROP CONSTRAINT IF EXISTS {check.name}")
        statements.append(f"{prefix} ADD CONSTRAINT {check.name} CHECK ({check.condition})")
    return statements


OPS_TABLE_DDL: Mapping[str, str] = {name: _create_table(t) for name, t in OPS_TABLES.items()}
# Every CHECK statement for empty tables; the snapshot pins this text.
OPS_CHECK_DDL: tuple[str, ...] = tuple(
    s for t in OPS_TABLES.values() for s in check_statements(t, {})
)


def render_ops_ddl() -> str:
    """Return every ops statement, in run order, as one SQL script."""
    return ";\n\n".join([*OPS_TABLE_DDL.values(), *OPS_CHECK_DDL]) + ";\n"
