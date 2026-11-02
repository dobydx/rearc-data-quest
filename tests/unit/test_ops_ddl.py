from dataclasses import fields
from pathlib import Path

import pytest

from quest.ingest.models import FileState, FileVersion, IngestRun, RunFile
from quest.ops_ddl import (
    OPS_CHECK_DDL,
    OPS_TABLE_DDL,
    OPS_TABLES,
    check_statements,
    render_ops_ddl,
)

SNAPSHOT = Path(__file__).parent / "snapshots" / "ops_ddl.sql"


def test_ddl_covers_all_ops_tables() -> None:
    assert set(OPS_TABLE_DDL) == {
        "ingest_runs",
        "ingest_run_files",
        "file_versions",
        "file_state",
        "table_versions",
        "dq_checks",
    }
    for name, ddl in OPS_TABLE_DDL.items():
        assert ddl.startswith(f"CREATE TABLE IF NOT EXISTS quest_ops.{name} (")
        assert f"CONSTRAINT {name}_pk PRIMARY KEY" in ddl


@pytest.mark.parametrize(
    ("table", "check"),
    [
        ("ingest_runs", "status IN ('running', 'succeeded', 'failed')"),
        ("file_state", "status IN ('active', 'missing', 'deleted')"),
        ("ingest_run_files", "action IN ('SKIPPED_FINGERPRINT', 'NOT_MODIFIED'"),
        ("dq_checks", "status IN ('pass', 'warn', 'fail', 'error')"),
        ("dq_checks", "severity IN ('warn', 'fail')"),
    ],
)
def test_ddl_has_check_on_status(table: str, check: str) -> None:
    (add,) = [s for s in OPS_CHECK_DDL if f"quest_ops.{table} ADD" in s and check in s]
    name = add.split("ADD CONSTRAINT ")[1].split()[0]

    drop = f"ALTER TABLE quest_ops.{table} DROP CONSTRAINT IF EXISTS {name}"
    assert OPS_CHECK_DDL.index(drop) == OPS_CHECK_DDL.index(add) - 1


@pytest.mark.parametrize(
    ("table", "record"),
    [
        ("ingest_runs", IngestRun),
        ("ingest_run_files", RunFile),
        ("file_versions", FileVersion),
        ("file_state", FileState),
    ],
)
def test_ddl_columns_match_ingest_records(table: str, record: type) -> None:
    columns = [column.name for column in OPS_TABLES[table].columns]

    assert sorted(columns) == sorted(f.name for f in fields(record))


def test_primary_key_columns_are_not_null() -> None:
    for table in OPS_TABLES.values():
        nullable = {c.name for c in table.columns if c.nullable}
        assert not nullable & set(table.key), table.name


def test_ddl_matches_snapshot() -> None:
    assert render_ops_ddl() == SNAPSHOT.read_text()


def test_check_statements_skip_a_constraint_already_in_place() -> None:
    table = OPS_TABLES["file_state"]
    (check,) = table.checks
    existing = {f"delta.constraints.{check.name}": check.condition, "other": "x"}

    assert check_statements(table, existing) == []


@pytest.mark.parametrize("existing", [{}, {"delta.constraints.file_state_status": "status = 'x'"}])
def test_check_statements_replace_an_absent_or_different_constraint(
    existing: dict[str, str],
) -> None:
    statements = check_statements(OPS_TABLES["file_state"], existing)

    assert statements == [
        "ALTER TABLE quest_ops.file_state DROP CONSTRAINT IF EXISTS file_state_status",
        "ALTER TABLE quest_ops.file_state ADD CONSTRAINT file_state_status "
        "CHECK (status IN ('active', 'missing', 'deleted'))",
    ]
