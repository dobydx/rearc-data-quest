from collections.abc import Callable, Iterator
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from quest.validate.checks import (
    CHECKS,
    CheckResult,
    TableVersion,
    ValidateContext,
    capture_table_versions,
    compare_to_previous,
    content_signature,
    data_tables,
    run_checks,
)
from tests.spark.conftest import (
    RUN_FILES_DDL,
    TABLE_VERSIONS_DDL,
    Silver,
    publish_gold,
    save,
)

pytestmark = pytest.mark.spark

NOW = datetime(2026, 11, 3, 16, 0, tzinfo=UTC)


@pytest.fixture
def ctx(spark: SparkSession, silver_catalog: Silver) -> ValidateContext:
    return ValidateContext(spark=spark, run_id="v1", ingest_pointers_flipped=0, now=lambda: NOW)


def results(ctx: ValidateContext, code: str) -> dict[str, CheckResult]:
    return {result.table_name: result for result in CHECKS[code](ctx)}


@pytest.fixture
def restore_quarantine(spark: SparkSession, silver_catalog: Silver) -> Iterator[None]:
    yield
    for table in ("datausa_population_quarantine", "bls_pr_class_quarantine"):
        schema = spark.table(f"quest_silver.{table}").schema
        save(spark, spark.createDataFrame([], schema), f"quest_silver.{table}")


def _quarantine_one(spark: SparkSession, table: str) -> None:
    """Write one quarantined row into a quarantine twin."""
    schema = spark.table(f"quest_silver.{table}").schema
    row: dict[str, object] = {field.name: "x" for field in schema.fields}
    row["dq_reasons"] = ["BAD_YEAR"]
    save(spark, spark.createDataFrame([row], schema), f"quest_silver.{table}")


def test_fixture_run_has_no_failures(ctx: ValidateContext) -> None:
    failed = [r for r in run_checks(ctx) if r.status in ("fail", "error")]

    assert failed == []


def test_parity_passes_on_fixture_gold(ctx: ValidateContext) -> None:
    parity = results(ctx, "PARITY_SQL_PYSPARK")

    assert len(parity) == 4
    assert {r.status for r in parity.values()} == {"pass"}


def test_parity_fails_on_tampered_gold(ctx: ValidateContext, spark: SparkSession) -> None:
    gold = spark.table("quest_gold.series_best_year")
    tampered = gold.withColumn("best_year", F.col("best_year") + 1).collect()
    save(spark, spark.createDataFrame(tampered, gold.schema), "quest_gold.series_best_year")
    try:
        parity = results(ctx, "PARITY_SQL_PYSPARK")["series_best_year"]
    finally:
        publish_gold(spark, "series_best_year")

    assert parity.status == "fail"
    assert parity.failing_rows == 10


def test_quarantine_rate_ignores_lookups(
    ctx: ValidateContext, spark: SparkSession, restore_quarantine: None
) -> None:
    _quarantine_one(spark, "bls_pr_class_quarantine")

    rate = results(ctx, "QUARANTINE_RATE")["bls_pr_class"]

    assert (rate.severity, rate.status, rate.failing_rows) == ("warn", "warn", 1)


def test_quarantine_rate_fails_above_1pct(
    ctx: ValidateContext, spark: SparkSession, restore_quarantine: None
) -> None:
    _quarantine_one(spark, "datausa_population_quarantine")

    rate = results(ctx, "QUARANTINE_RATE")["datausa_population"]

    assert (rate.status, rate.failing_rows) == ("fail", 1)
    assert rate.threshold == "0.01"


def test_quarantine_rate_passes_when_empty(ctx: ValidateContext) -> None:
    assert {r.status for r in results(ctx, "QUARANTINE_RATE").values()} == {"pass"}


def test_current_subset_check_passes(ctx: ValidateContext) -> None:
    subset = results(ctx, "CURRENT_NOT_SUBSET")["bls_pr_observations"]
    values = results(ctx, "CURRENT_VALUE_MISMATCH")["bls_pr_observations"]

    assert (subset.status, subset.failing_rows) == ("pass", 0)
    assert (values.status, values.failing_rows) == ("pass", 0)


def test_reconcile_bronze_passes(ctx: ValidateContext) -> None:
    reconcile = results(ctx, "RECONCILE_BRONZE")

    assert {table: r.status for table, r in reconcile.items()} == {
        "bls_pr_observations": "pass",
        "datausa_population": "pass",
    }


def test_gold_rowcount_matches_series(ctx: ValidateContext) -> None:
    counts = results(ctx, "GOLD_ROWCOUNT")

    assert {r.status for r in counts.values()} == {"pass"}
    assert counts["series_best_year"].observed == "6 rows, expected 6"


def test_stale_source_warns_after_100_days(spark: SparkSession, silver_catalog: Silver) -> None:
    later = ValidateContext(spark, "v1", 0, now=lambda: datetime(2027, 1, 1, tzinfo=UTC))

    stale = results(later, "STALE_SOURCE")["bls_pr_observations"]

    assert (stale.severity, stale.status) == ("warn", "warn")


WriteVersions = Callable[..., None]


@pytest.fixture
def table_versions(spark: SparkSession, silver_catalog: Silver) -> Iterator[WriteVersions]:
    """Replace quest_ops.table_versions with one bls_pr_series row per (run_id, changed)."""

    def write(*runs: tuple[str, bool]) -> None:
        versions = [
            TableVersion(run_id, "quest_silver.bls_pr_series", "silver", 3, 6, Decimal(42), ch, NOW)
            for run_id, ch in runs
        ]
        save(
            spark,
            spark.createDataFrame([asdict(v) for v in versions], TABLE_VERSIONS_DDL),
            "quest_ops.table_versions",
        )

    yield write
    save(spark, spark.createDataFrame([], TABLE_VERSIONS_DDL), "quest_ops.table_versions")


def test_no_change_on_rerun_fails_when_version_moves(
    ctx: ValidateContext, table_versions: WriteVersions
) -> None:
    table_versions(("v0", True), ("v1", True))

    (result,) = CHECKS["NO_CHANGE_ON_RERUN"](ctx)

    assert (result.status, result.failing_rows, result.observed) == ("fail", 1, "1 table changed")
    assert result.details is not None
    assert "quest_silver.bls_pr_series" in result.details


def test_no_change_on_rerun_warns_when_a_table_has_no_previous_capture(
    ctx: ValidateContext, table_versions: WriteVersions
) -> None:
    # With no baseline the check can neither prove "no change" nor fail on it.
    table_versions(("v1", True))

    (result,) = CHECKS["NO_CHANGE_ON_RERUN"](ctx)

    assert (result.status, result.failing_rows) == ("warn", 0)
    assert result.details == "tables with no previous capture: 1"


def test_no_change_on_rerun_passes_when_ingest_landed(
    spark: SparkSession, table_versions: WriteVersions
) -> None:
    table_versions(("v2", True))
    landed = ValidateContext(spark, "v2", ingest_pointers_flipped=1, now=lambda: NOW)

    (result,) = CHECKS["NO_CHANGE_ON_RERUN"](landed)

    assert result.status == "pass"


def test_no_change_on_rerun_expects_change_when_the_code_version_changed(
    spark: SparkSession, table_versions: WriteVersions
) -> None:
    # The gate reruns the pipeline on a new code version, so moved tables are expected.
    table_versions(("v0", True), ("v1", True))
    new_code = ValidateContext(spark, "v1", ingest_pointers_flipped=0, now=lambda: NOW,
                               code_changed=True)  # fmt: skip

    (result,) = CHECKS["NO_CHANGE_ON_RERUN"](new_code)

    assert (result.status, result.observed) == ("pass", "change expected: code version changed")
    assert result.details is not None
    assert "quest_silver.bls_pr_series" in result.details


def test_compare_to_previous_needs_version_and_content_to_move() -> None:
    previous = {"t": TableVersion("v0", "t", "silver", 3, 6, Decimal(42), False, NOW)}
    same_content = TableVersion("v1", "t", "silver", 4, 6, Decimal(42), False, NOW)
    new_content = TableVersion("v1", "t", "silver", 4, 7, Decimal(43), False, NOW)
    no_history = TableVersion("v1", "u", "silver", 0, 1, Decimal(1), False, NOW)

    compared = compare_to_previous([same_content, new_content, no_history], previous)

    assert [v.changed_since_previous for v in compared] == [False, True, True]


def test_compare_to_previous_judges_content_alone_when_a_version_is_unknown() -> None:
    # Materialized views have no readable Delta history, so their version is NULL.
    previous = {
        name: TableVersion("v0", name, "gold", None, 6, Decimal(42), False, NOW)
        for name in ("same", "new")
    }
    same_content = TableVersion("v1", "same", "gold", None, 6, Decimal(42), False, NOW)
    new_content = TableVersion("v1", "new", "gold", None, 6, Decimal(43), False, NOW)

    compared = compare_to_previous([same_content, new_content], previous)

    assert [v.changed_since_previous for v in compared] == [False, True]


def test_capture_never_reads_history_of_a_materialized_view(
    ctx: ValidateContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Locally no table is Delta, so DESCRIBE HISTORY on any of these would raise.
    views = [(table, layer) for table, layer in data_tables() if layer != "bronze"]
    monkeypatch.setattr("quest.validate.checks.data_tables", lambda: views)

    captured = capture_table_versions(ctx)

    assert {v.delta_version for v in captured} == {None}
    assert len(captured) == len(views)


@pytest.mark.connect_safe
def test_content_signature_is_order_independent(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, "a"), (2, "b")], "x INT, y STRING")

    assert content_signature(df) == content_signature(df.orderBy(F.col("x").desc()))
    assert content_signature(df)[0] == 2
    assert content_signature(df) != content_signature(df.limit(1))


@pytest.fixture
def file_versions(spark: SparkSession, silver_catalog: Silver) -> Iterator[Callable[..., None]]:
    """Rewrite the AllData row of quest_ops.file_versions; restore it afterwards."""
    table = spark.table("quest_ops.file_versions")
    original = table.collect()

    def rewrite(**changes: object) -> None:
        rows = [
            {**row.asDict(), **changes} if row["file_name"] == "pr.data.1.AllData" else row.asDict()
            for row in original
        ]
        save(spark, spark.createDataFrame(rows, table.schema), "quest_ops.file_versions")

    yield rewrite
    save(spark, spark.createDataFrame(original, table.schema), "quest_ops.file_versions")


def test_stale_source_without_last_modified_is_an_error(
    ctx: ValidateContext, file_versions: Callable[..., None]
) -> None:
    file_versions(http_last_modified=None)

    stale = results(ctx, "STALE_SOURCE")["bls_pr_observations"]

    assert stale.status == "error"
    assert stale.details is not None


def test_reconcile_bronze_compares_file_line_count(
    ctx: ValidateContext, file_versions: Callable[..., None]
) -> None:
    file_versions(line_count=1400)

    reconcile = results(ctx, "RECONCILE_BRONZE")["bls_pr_observations"]

    assert (reconcile.status, reconcile.failing_rows) == ("fail", 9)
    assert "file lines 1400" in reconcile.observed


@pytest.fixture
def alldata_reverted(spark: SparkSession, silver_catalog: Silver) -> Iterator[None]:
    """AllData went A -> B -> A: B (first seen last, 2000 lines) is the previous served version."""
    versions = spark.table("quest_ops.file_versions")
    original = versions.collect()
    (current,) = [r for r in original if r["file_name"] == "pr.data.1.AllData"]
    a, b = current["sha256"], "b" * 64
    later = current["first_seen_at"] + timedelta(days=30)
    version_b = current.asDict() | {"sha256": b, "line_count": 2000, "first_seen_at": later}
    reverted = [*original, version_b]
    save(spark, spark.createDataFrame(reverted, versions.schema), "quest_ops.file_versions")
    t0 = datetime(2026, 9, 1)
    flips = [
        ("r1", "bls_pr", "pr.data.1.AllData", None, a, True, t0),
        ("r2", "bls_pr", "pr.data.1.AllData", a, b, True, t0 + timedelta(days=1)),
        ("r3", "bls_pr", "pr.data.1.AllData", b, b, False, t0 + timedelta(days=2)),
        ("r4", "bls_pr", "pr.data.1.AllData", b, a, True, t0 + timedelta(days=3)),
    ]
    save(spark, spark.createDataFrame(flips, RUN_FILES_DDL), "quest_ops.ingest_run_files")
    yield
    save(spark, spark.createDataFrame(original, versions.schema), "quest_ops.file_versions")
    save(spark, spark.createDataFrame([], RUN_FILES_DDL), "quest_ops.ingest_run_files")


def test_row_count_shrink_compares_with_the_previously_served_version(
    ctx: ValidateContext, alldata_reverted: None
) -> None:
    # First-seen order would call A the oldest version and find no previous one.
    shrink = results(ctx, "ROW_COUNT_SHRINK")["bls_pr_observations"]

    assert shrink.observed.startswith("2000 -> ")
    assert shrink.status == "fail"
