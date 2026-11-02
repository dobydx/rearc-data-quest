from datetime import datetime

import pytest

from quest.ingest.delta_ops_store import catalog_identifier, file_state_from_row, row_values
from quest.ingest.models import FileStatus
from tests.unit.factories import state


def test_row_values_follow_table_column_order_and_unwrap_enums() -> None:
    values = row_values(state("pr.series"), ["status", "file_name", "consecutive_missing"])

    assert values == ("active", "pr.series", 0)
    assert type(values[0]) is str


def test_row_values_fail_on_unknown_column() -> None:
    with pytest.raises(KeyError, match="no_such_column"):
        row_values(state("pr.series"), ["no_such_column"])


def test_file_state_from_row_restores_enum_and_utc() -> None:
    expected = state("pr.series")
    row = {
        **{k: getattr(expected, k) for k in expected.__dataclass_fields__},
        "status": "active",
        # Spark hands TIMESTAMP back as naive datetimes in the session time zone (UTC).
        "http_last_modified": datetime(2026, 9, 3, 12, 30),
        "updated_at": datetime(2026, 9, 3, 12, 30),
    }

    restored = file_state_from_row(row)

    assert restored == expected
    assert restored.status is FileStatus.ACTIVE


@pytest.mark.parametrize("name", ["workspace", "dev_catalog", "Cat1"])
def test_catalog_identifier_quotes_valid_names(name: str) -> None:
    assert catalog_identifier(name) == f"`{name}`"


@pytest.mark.parametrize("name", ["", "a b", "x`; DROP TABLE y", "a.b"])
def test_catalog_identifier_rejects_unsafe_names(name: str) -> None:
    with pytest.raises(ValueError, match="catalog"):
        catalog_identifier(name)
