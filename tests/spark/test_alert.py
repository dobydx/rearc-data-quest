from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from tests.spark.conftest import save

pytestmark = pytest.mark.spark

ALERT_SQL = Path(__file__).parents[2] / "src" / "sql" / "alert_dq.sql"
# A database of its own, so these tables never replace the shared fixture catalog's quest_ops.
OPS = "alert_test_ops"
NOW = datetime.now(UTC).replace(tzinfo=None)


def _issues(
    spark: SparkSession,
    checks: list[tuple[str, str, datetime]],
    files: list[tuple[str, datetime | None]],
) -> int:
    save(spark, spark.createDataFrame(checks, "run_id STRING, status STRING, checked_at TIMESTAMP"),
         f"{OPS}.dq_checks")  # fmt: skip
    save(spark, spark.createDataFrame(files, "file_name STRING, http_last_modified TIMESTAMP"),
         f"{OPS}.file_state")  # fmt: skip
    (row,) = spark.sql(ALERT_SQL.read_text().replace("quest_ops.", f"{OPS}.")).collect()
    issues: int = row["issues"]
    return issues


def _checks(*latest_statuses: str) -> list[tuple[str, str, datetime]]:
    old = [("run-1", "fail", NOW - timedelta(days=7))]
    return old + [("run-2", status, NOW) for status in latest_statuses]


FRESH = [("pr.data.1.AllData", NOW - timedelta(days=10)), ("pr.txt", NOW - timedelta(days=5000))]


def test_quiet_when_the_latest_run_passed_and_alldata_is_fresh(spark: SparkSession) -> None:
    assert _issues(spark, _checks("pass", "pass"), FRESH) == 0


def test_counts_warn_fail_and_error_of_the_latest_run_only(spark: SparkSession) -> None:
    assert _issues(spark, _checks("pass", "warn", "fail", "error"), FRESH) == 3


def test_fires_when_alldata_is_older_than_100_days(spark: SparkSession) -> None:
    stale = [("pr.data.1.AllData", NOW - timedelta(days=101))]

    assert _issues(spark, _checks("pass"), stale) == 1


def test_fires_when_alldata_has_no_state_row(spark: SparkSession) -> None:
    assert _issues(spark, _checks("pass"), [("pr.txt", NOW)]) == 1
