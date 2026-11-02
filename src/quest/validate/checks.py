"""Validate checks. One function per rule code in rules.yml; each runs once per rule (table)."""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

from pyspark.sql import Column, DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from quest.config import utc_now
from quest.contracts.loader import (
    contract_by_role,
    contract_by_silver_table,
    contract_for_file,
    load_contracts,
    load_rules,
    silver_contracts,
    silver_table_of,
)
from quest.contracts.models import FileContract, Rule
from quest.gold.pyspark_gold import GOLD_BUILDERS, Q3_PERIOD, Q3_SERIES_ID, build_gold
from quest.ingest.models import SERVED_STATUSES
from quest.log import get_logger
from quest.sql import sql_string

CheckSeverity = Literal["warn", "fail"]
CheckStatus = Literal["pass", "warn", "fail", "error"]

QUARANTINE_RATE_LIMIT = 0.01
SHRINK_WARN_ABOVE = 0.01
SHRINK_FAIL_ABOVE = 0.10
STALE_AFTER_DAYS = 100
PARITY_REL_TOL = 1e-9

SILVER = "quest_silver"
GOLD = "quest_gold"
BRONZE = "quest_bronze"
OPS = "quest_ops"
QUARANTINE_SUFFIX = "_quarantine"
_SERVED = ", ".join(sql_string(status.value) for status in SERVED_STATUSES)


@dataclass(frozen=True)
class CheckResult:
    """Outcome of one check on one table."""

    code: str
    table_name: str
    severity: CheckSeverity
    status: CheckStatus
    observed: str
    threshold: str | None
    failing_rows: int | None
    details: str | None


@dataclass(frozen=True)
class ValidateContext:
    """What every check reads: the session, the run, and what ingest changed."""

    spark: SparkSession
    run_id: str
    ingest_pointers_flipped: int
    now: Callable[[], datetime] = utc_now
    # True when the pipeline ran new code, which may rebuild tables with no new data.
    code_changed: bool = False


@dataclass(frozen=True)
class TableVersion:
    """One ``quest_ops.table_versions`` row: a data table's version and content this run."""

    run_id: str
    table_name: str
    layer: str
    delta_version: int | None  # None for a materialized view, which has no readable history
    row_count: int
    content_checksum: Decimal
    changed_since_previous: bool
    captured_at: datetime


@dataclass(frozen=True)
class DqCheck:
    """One ``quest_ops.dq_checks`` row."""

    run_id: str
    check_code: str
    table_name: str
    layer: str
    severity: str
    status: str
    observed_value: str
    threshold: str | None
    failing_rows: int | None
    details: str | None
    checked_at: datetime


def layer_of(table: str) -> str:
    """Return ``silver``, ``gold`` or ``ops`` for a table name used in rules.yml."""
    silver = {silver_table_of(c) for c in silver_contracts()}
    if table.removesuffix(QUARANTINE_SUFFIX) in silver:
        return "silver"
    return "gold" if table in GOLD_BUILDERS else "ops"


def dq_check_row(result: CheckResult, run_id: str, checked_at: datetime) -> DqCheck:
    """Shape a result as a ``dq_checks`` row."""
    return DqCheck(
        run_id=run_id,
        check_code=result.code,
        table_name=result.table_name,
        layer=layer_of(result.table_name),
        severity=result.severity,
        status=result.status,
        observed_value=result.observed,
        threshold=result.threshold,
        failing_rows=result.failing_rows,
        details=result.details,
        checked_at=checked_at,
    )


# ---- result helpers ---------------------------------------------------------------------


def _result(
    rule: Rule,
    failing: int,
    observed: str,
    *,
    threshold: str | None = None,
    details: str | None = None,
    status: CheckStatus | None = None,
) -> CheckResult:
    """A result whose status is the rule's severity when anything fails, unless given."""
    return CheckResult(
        code=rule.code,
        table_name=rule.table,
        severity=_severity(rule),
        status=status or ("pass" if failing == 0 else _severity(rule)),
        observed=observed,
        threshold=threshold,
        failing_rows=failing,
        details=details,
    )


def _severity(rule: Rule) -> CheckSeverity:
    return "fail" if rule.severity == "fail" else "warn"


def _graded(value: float, warn_above: float, fail_above: float) -> CheckStatus:
    if value > fail_above:
        return "fail"
    return "warn" if value > warn_above else "pass"


# ---- data access ------------------------------------------------------------------------


def _count(ctx: ValidateContext, sql: str, **args: Any) -> int:
    return _count_frame(ctx.spark.sql(sql, args=args or None))


def _count_frame(df: DataFrame) -> int:
    """The first column of the first row as an int; 0 when empty or NULL."""
    row = df.first()
    return int(row[0]) if row is not None and row[0] is not None else 0


def _current(ctx: ValidateContext, contract: FileContract) -> Row | None:
    """The served version of a contract's file: name, sha, line count, release age."""
    return ctx.spark.sql(
        f"""
        SELECT s.file_name, v.sha256, v.line_count,
               datediff(:now, v.http_last_modified) AS age_days,
               to_date(v.http_last_modified) AS released_on
        FROM {OPS}.file_state AS s
        JOIN {OPS}.file_versions AS v
          ON v.source = s.source AND v.file_name = s.file_name AND v.sha256 = s.current_sha256
        WHERE s.source = :source AND s.status IN ({_SERVED}) AND v.header_ok
          AND s.file_name RLIKE :pattern
        """,
        args={"now": ctx.now(), "source": contract.source, "pattern": contract.file_pattern},
    ).first()


def _previous(ctx: ValidateContext, current: Row, source: str) -> Row | None:
    """The version served before ``current``, read from the latest pointer flip to it.

    Release order is pointer-flip order, not first-seen order: after A -> B -> A, the version
    before A is B, although A was seen first.
    """
    return ctx.spark.sql(
        f"""
        SELECT f.sha256_before AS previous_sha256, v.line_count AS previous_line_count
        FROM {OPS}.ingest_run_files AS f
        JOIN {OPS}.file_versions AS v
          ON v.source = f.source AND v.file_name = f.file_name AND v.sha256 = f.sha256_before
        WHERE f.source = :source AND f.file_name = :file_name AND f.pointer_flipped
          AND f.sha256_after = :sha256 AND f.sha256_before <> f.sha256_after AND v.header_ok
        ORDER BY f.recorded_at DESC
        LIMIT 1
        """,
        args={"source": source, "file_name": current["file_name"], "sha256": current["sha256"]},
    ).first()


def _missing(rule: Rule, what: str) -> CheckResult:
    return _result(rule, 0, "n/a", status="error", details=f"{what} not found")


# ---- file and release checks ------------------------------------------------------------


def _stale_source(ctx: ValidateContext, rule: Rule) -> CheckResult:
    current = _current(ctx, contract_by_silver_table(rule.table))
    if current is None:
        return _missing(rule, "served version")
    if current["age_days"] is None:
        return _missing(rule, "Last-Modified of the served version")
    age = int(current["age_days"])
    return _result(
        rule, int(age > STALE_AFTER_DAYS), f"{age} days", threshold=str(STALE_AFTER_DAYS)
    )


def _mixed_release(ctx: ValidateContext, rule: Rule) -> CheckResult:
    data, series = (
        _current(ctx, contract_by_silver_table(rule.table)),
        _current(ctx, contract_by_role("series")),
    )
    if data is None or series is None:
        return _missing(rule, "served pr.series or data version")
    observed = f"data {data['released_on']}, series {series['released_on']}"
    return _result(rule, int(data["released_on"] != series["released_on"]), observed)


def _row_count_shrink(ctx: ValidateContext, rule: Rule) -> CheckResult:
    contract = contract_by_silver_table(rule.table)
    current = _current(ctx, contract)
    if current is None:
        return _missing(rule, "served version")
    previous = _previous(ctx, current, contract.source)
    if previous is None:
        return _result(rule, 0, "no previous version")
    before, now = int(previous["previous_line_count"]), int(current["line_count"])
    shrink = (before - now) / before if before else 0.0
    return _result(
        rule,
        max(before - now, 0),
        f"{before} -> {now} lines ({shrink:.2%} shrink)",
        threshold=f"warn {SHRINK_WARN_ABOVE}, fail {SHRINK_FAIL_ABOVE}",
        status=_graded(shrink, SHRINK_WARN_ABOVE, SHRINK_FAIL_ABOVE),
    )


def _unmodelled_file(ctx: ValidateContext, rule: Rule) -> CheckResult:
    rows = ctx.spark.sql(
        f"SELECT file_name FROM {OPS}.file_state WHERE status = 'active'"
    ).collect()
    unmodelled = sorted(r["file_name"] for r in rows if contract_for_file(r["file_name"]) is None)
    return _result(
        rule, len(unmodelled), f"{len(unmodelled)} files", details=", ".join(unmodelled) or None
    )


# ---- observation and series checks ------------------------------------------------------

_OBS = f"{SILVER}.bls_pr_observations"
_SERIES = f"{SILVER}.bls_pr_series"


def _counting(sql: str, what: str) -> Callable[[ValidateContext, Rule], CheckResult]:
    """A check that fails one row per result of ``sql``."""

    def check(ctx: ValidateContext, rule: Rule) -> CheckResult:
        failing = _count(ctx, sql)
        return _result(rule, failing, f"{failing} {what}")

    return check


_outside_series_range = _counting(
    f"""
    SELECT count(*) FROM {_OBS} AS o JOIN {_SERIES} AS s USING (series_id)
    WHERE concat(CAST(o.year AS STRING), o.period)
      NOT BETWEEN concat(CAST(s.begin_year AS STRING), s.begin_period)
          AND concat(CAST(s.end_year AS STRING), s.end_period)
    """,
    "observations outside their series range",
)
_quarter_on_annual_series = _counting(
    f"""
    SELECT count(*) FROM {_OBS} AS o JOIN {_SERIES} AS s USING (series_id)
    WHERE s.frequency = 'ANNUAL' AND NOT o.is_annual_average
    """,
    "quarterly rows on annual-only series",
)
_q05_completeness = _counting(
    f"""
    SELECT count(*) FROM (
      SELECT o.series_id, o.year,
             count_if(NOT o.is_annual_average) AS quarters,
             bool_or(o.is_annual_average) AS has_annual_average
      FROM {_OBS} AS o JOIN {_SERIES} AS s USING (series_id)
      WHERE s.frequency = 'QUARTERLY'
      GROUP BY o.series_id, o.year
    ) WHERE (quarters = 4) <> has_annual_average
    """,
    "series-years where Q05 and 4 quarters disagree",
)
_internal_quarter_gap = _counting(
    f"""
    SELECT count(*) FROM (
      SELECT series_id,
             max(year * 4 + quarter_number) - min(year * 4 + quarter_number) + 1 - count(*) AS gaps
      FROM {_OBS} WHERE NOT is_annual_average GROUP BY series_id
    ) WHERE gaps > 0
    """,
    "series with a missing quarter",
)
_series_range_consistency = _counting(
    f"""
    SELECT count(*) FROM {_SERIES} AS s JOIN (
      SELECT o.series_id,
             min(concat(CAST(o.year AS STRING), o.period)) AS first_key,
             max(concat(CAST(o.year AS STRING), o.period)) AS last_key
      FROM {_OBS} AS o JOIN {_SERIES} AS f USING (series_id)
      WHERE o.is_annual_average = (f.frequency = 'ANNUAL')
      GROUP BY o.series_id
    ) AS o USING (series_id)
    WHERE o.first_key <> concat(CAST(s.begin_year AS STRING), s.begin_period)
       OR o.last_key <> concat(CAST(s.end_year AS STRING), s.end_period)
    """,
    "series whose begin or end differs from the data",
)
_series_without_data = _counting(
    f"SELECT count(*) FROM {_SERIES} AS s LEFT ANTI JOIN {_OBS} AS o USING (series_id)",
    "series without observations",
)
_base_year_unit_mismatch = _counting(
    f"SELECT count(*) FROM {_SERIES} WHERE (unit_of_measure = 'INDEX') <> (base_year IS NOT NULL)",
    "series where index and base year disagree",
)
_base_year_text_mismatch = _counting(
    f"""
    SELECT count(*) FROM {_SERIES}
    WHERE unit_of_measure = 'INDEX'
      AND NOT coalesce(
        instr(duration_text, concat('(', CAST(base_year AS STRING), '=100)')) > 0, false
      )
    """,
    "index series whose duration text names another base year",
)
_annual_flag_inconsistent = _counting(
    f"SELECT count(*) FROM {_SERIES} WHERE (begin_period = 'Q05') <> (end_period = 'Q05')",
    "series with only one end on Q05",
)

_CURRENT_ROWS = f"""
    WITH current_rows AS (
      SELECT trim(series_id) AS series_id, try_cast(trim(year) AS INT) AS year,
             trim(period) AS period, try_cast(trim(value) AS DECIMAL(12,3)) AS value
      FROM {BRONZE}.bls_pr_data WHERE _file_name = :file_name AND _sha256 = :sha256
    )
"""


def _current_vs_alldata(join: str, what: str) -> Callable[[ValidateContext, Rule], CheckResult]:
    """Compare the served pr.data.0.Current rows with silver observations."""

    def check(ctx: ValidateContext, rule: Rule) -> CheckResult:
        current = _current(ctx, contract_by_role("observations_current"))
        if current is None:
            return _result(rule, 0, "no served pr.data.0.Current")
        sql = f"{_CURRENT_ROWS} SELECT count(*) FROM current_rows AS c {join}"
        failing = _count(ctx, sql, file_name=current["file_name"], sha256=current["sha256"])
        return _result(rule, failing, f"{failing} {what}")

    return check


_current_not_subset = _current_vs_alldata(
    f"LEFT ANTI JOIN {_OBS} AS o "
    "ON o.series_id = c.series_id AND o.year = c.year AND o.period = c.period",
    "Current keys missing from AllData",
)
_current_value_mismatch = _current_vs_alldata(
    f"JOIN {_OBS} AS o ON o.series_id = c.series_id AND o.year = c.year AND o.period = c.period "
    "WHERE c.value IS DISTINCT FROM o.value",
    "values that differ between Current and AllData",
)


# ---- silver reconciliation and quarantine -----------------------------------------------


def _reconcile_bronze(ctx: ValidateContext, rule: Rule) -> CheckResult:
    contract = contract_by_silver_table(rule.table)
    current = _current(ctx, contract)
    if current is None:
        return _missing(rule, "served version")
    args = {"file_name": current["file_name"], "sha256": current["sha256"]}
    bronze = ctx.spark.table(f"{BRONZE}.{contract.bronze_table}").where(
        (F.col("_file_name") == args["file_name"]) & (F.col("_sha256") == args["sha256"])
    )
    bronze_rows = bronze.count()
    silver_rows = ctx.spark.table(f"{SILVER}.{rule.table}").count()
    quarantine_rows = ctx.spark.table(f"{SILVER}.{rule.table}{QUARANTINE_SUFFIX}").count()
    file_lines = int(current["line_count"])
    # Bytes to bronze (lines ingest counted) and bronze to silver (nothing dropped).
    failing = abs(file_lines - bronze_rows) + abs(bronze_rows - silver_rows - quarantine_rows)
    observed = (
        f"file lines {file_lines}, bronze {bronze_rows}, silver {silver_rows}, "
        f"quarantine {quarantine_rows}"
    )
    if "page_total" in bronze.columns:
        # DataUSA states its own row count; a truncated response would disagree with it.
        page_total = _count_frame(bronze.agg(F.max(F.col("page_total").cast("bigint"))))
        failing += abs(bronze_rows - page_total)
        observed += f", page.total {page_total}"
    return _result(rule, failing, observed)


def _duplicate_key(ctx: ValidateContext, rule: Rule) -> CheckResult:
    failing = (
        ctx.spark.table(f"{SILVER}.{rule.table}{QUARANTINE_SUFFIX}")
        .where(F.array_contains("dq_reasons", "DUPLICATE_KEY"))
        .count()
    )
    return _result(rule, failing, f"{failing} rows quarantined as DUPLICATE_KEY")


def _quarantine_rate(ctx: ValidateContext, rule: Rule) -> CheckResult:
    quarantined = ctx.spark.table(f"{SILVER}.{rule.table}{QUARANTINE_SUFFIX}").count()
    total = quarantined + ctx.spark.table(f"{SILVER}.{rule.table}").count()
    rate = quarantined / total if total else 0.0
    gated = rule.severity == "fail"
    status: CheckStatus = (
        "pass" if quarantined == 0 else "fail" if gated and rate > QUARANTINE_RATE_LIMIT else "warn"
    )
    return _result(
        rule,
        quarantined,
        f"{quarantined} of {total} rows ({rate:.2%})",
        threshold=str(QUARANTINE_RATE_LIMIT) if gated else None,
        status=status,
    )


def _pop_years_shrank(ctx: ValidateContext, rule: Rule) -> CheckResult:
    contract = contract_by_silver_table(rule.table)
    current = _current(ctx, contract)
    if current is None:
        return _missing(rule, "served version")
    previous = _previous(ctx, current, contract.source)
    if previous is None:
        return _result(rule, 0, "no previous version")
    years = (
        f"SELECT DISTINCT trim(year) AS year FROM {BRONZE}.{contract.bronze_table} WHERE _sha256 ="
    )
    lost = _count(
        ctx,
        f"SELECT count(*) FROM ({years} :previous) AS p "
        f"LEFT ANTI JOIN ({years} :current) AS c USING (year)",
        previous=previous["previous_sha256"],
        current=current["sha256"],
    )
    return _result(rule, lost, f"{lost} years dropped since the previous version")


# ---- gold checks ------------------------------------------------------------------------


def gold_mismatches(expected: DataFrame, actual: DataFrame, rel_tol: float) -> int:
    """Count rows that differ between two frames; DOUBLE columns match within ``rel_tol``.

    Raises:
        ValueError: If the frames do not have the same columns and types.
    """
    columns = sorted(expected.columns)
    expected, actual = expected.select(*columns), actual.select(*columns)
    if expected.dtypes != actual.dtypes:
        raise ValueError(f"schemas differ: {expected.dtypes} vs {actual.dtypes}")
    approximate = [name for name, dtype in expected.dtypes if dtype in ("double", "float")]
    if not approximate:
        return expected.exceptAll(actual).count() + actual.exceptAll(expected).count()
    exact = [c for c in columns if c not in approximate]
    e, a = expected.alias("e"), actual.alias("a")
    on = [F.col(f"e.{c}").eqNullSafe(F.col(f"a.{c}")) for c in exact] or [F.lit(True)]
    close = [_close(F.col(f"e.{c}"), F.col(f"a.{c}"), rel_tol) for c in approximate]
    joined = e.join(a, on, "full_outer")
    return joined.where(~F.coalesce(_all(close), F.lit(False))).count()


def _close(left: Column, right: Column, rel_tol: float) -> Column:
    both_null = left.isNull() & right.isNull()
    tolerance = F.lit(rel_tol) * F.greatest(F.abs(left), F.abs(right))
    return both_null | (F.abs(left - right) <= tolerance)


def _all(conditions: Sequence[Column]) -> Column:
    combined = conditions[0]
    for condition in conditions[1:]:
        combined = combined & condition
    return combined


def _parity(ctx: ValidateContext, rule: Rule) -> CheckResult:
    sql_table = ctx.spark.table(f"{GOLD}.{rule.table}")
    twin = build_gold(rule.table, lambda table: ctx.spark.table(f"{SILVER}.{table}"))
    failing = gold_mismatches(sql_table, twin, PARITY_REL_TOL)
    return _result(rule, failing, f"{failing} differing rows", threshold=str(PARITY_REL_TOL))


_GOLD_EXPECTED_ROWS: Mapping[str, str] = {
    "series_best_year": f"SELECT count(*) FROM {_SERIES}",
    "prs30006032_q01_with_population": (
        f"SELECT count(*) FROM {_OBS} WHERE series_id = '{Q3_SERIES_ID}' AND period = '{Q3_PERIOD}'"
    ),
}


def _gold_rowcount(ctx: ValidateContext, rule: Rule) -> CheckResult:
    actual = ctx.spark.table(f"{GOLD}.{rule.table}").count()
    expected = _count(ctx, _GOLD_EXPECTED_ROWS[rule.table])
    return _result(rule, abs(actual - expected), f"{actual} rows, expected {expected}")


def _q1_incomplete(ctx: ValidateContext, rule: Rule) -> CheckResult:
    row = ctx.spark.table(f"{GOLD}.{rule.table}").first()
    if row is None:
        return _missing(rule, "Q1 row")
    return _result(rule, int(not row["is_complete"]), f"{row['n_years']} years in the window")


def _no_change_on_rerun(ctx: ValidateContext, rule: Rule) -> CheckResult:
    if ctx.ingest_pointers_flipped > 0:
        return _result(
            rule, 0, f"ingest changed {ctx.ingest_pointers_flipped} pointers; change expected"
        )
    # A table's first capture counts as changed, but with no baseline it proves nothing either
    # way: it is not a failure, and the run is reported as a warning (not proven).
    rows = ctx.spark.sql(
        f"""
        SELECT cur.table_name, prev.table_name IS NOT NULL AS has_baseline
        FROM {OPS}.table_versions AS cur
        LEFT JOIN (
          SELECT DISTINCT table_name FROM {OPS}.table_versions WHERE run_id <> :run_id
        ) AS prev ON prev.table_name = cur.table_name
        WHERE cur.run_id = :run_id AND cur.changed_since_previous
        """,
        args={"run_id": ctx.run_id},
    ).collect()
    changed = sorted(row["table_name"] for row in rows if row["has_baseline"])
    first_seen = len(rows) - len(changed)
    notes = []
    if changed:
        notes.append(", ".join(changed))
    if first_seen:
        notes.append(f"tables with no previous capture: {first_seen}")
    details = "; ".join(notes) or None
    unproven: CheckStatus | None = "warn" if first_seen and not changed else None
    if ctx.code_changed:
        return _result(rule, 0, "change expected: code version changed", details=details)
    noun = "table" if len(changed) == 1 else "tables"
    return _result(
        rule, len(changed), f"{len(changed)} {noun} changed", details=details, status=unproven
    )


# ---- registry and runners ---------------------------------------------------------------

RuleCheck = Callable[[ValidateContext, Rule], CheckResult]

_RULE_CHECKS: Mapping[str, RuleCheck] = {
    "STALE_SOURCE": _stale_source,
    "MIXED_RELEASE": _mixed_release,
    "ROW_COUNT_SHRINK": _row_count_shrink,
    "UNMODELLED_FILE": _unmodelled_file,
    "OUTSIDE_SERIES_RANGE": _outside_series_range,
    "QUARTER_ON_ANNUAL_SERIES": _quarter_on_annual_series,
    "Q05_COMPLETENESS": _q05_completeness,
    "INTERNAL_QUARTER_GAP": _internal_quarter_gap,
    "SERIES_RANGE_CONSISTENCY": _series_range_consistency,
    "SERIES_WITHOUT_DATA": _series_without_data,
    "CURRENT_NOT_SUBSET": _current_not_subset,
    "CURRENT_VALUE_MISMATCH": _current_value_mismatch,
    "RECONCILE_BRONZE": _reconcile_bronze,
    "DUPLICATE_KEY": _duplicate_key,
    "QUARANTINE_RATE": _quarantine_rate,
    "BASE_YEAR_UNIT_MISMATCH": _base_year_unit_mismatch,
    "BASE_YEAR_TEXT_MISMATCH": _base_year_text_mismatch,
    "ANNUAL_FLAG_INCONSISTENT": _annual_flag_inconsistent,
    "POP_YEARS_SHRANK": _pop_years_shrank,
    "Q1_INCOMPLETE": _q1_incomplete,
    "PARITY_SQL_PYSPARK": _parity,
    "GOLD_ROWCOUNT": _gold_rowcount,
    "NO_CHANGE_ON_RERUN": _no_change_on_rerun,
}


def _for_each_rule(code: str, check: RuleCheck) -> Callable[[ValidateContext], list[CheckResult]]:
    """Run ``check`` once per validate rule with ``code``; any exception becomes an error result."""

    def run(ctx: ValidateContext) -> list[CheckResult]:
        results = []
        for rule in (r for r in load_rules() if r.code == code and r.stage == "validate"):
            try:
                results.append(check(ctx, rule))
            # One broken check must not stop the others; its error is recorded and fails validate.
            except Exception as exc:  # noqa: BLE001
                error = f"{type(exc).__name__}: {exc}"
                get_logger(__name__).error("check_error", code=code, table=rule.table, error=error)
                results.append(_result(rule, 0, "n/a", status="error", details=error[:1000]))
        return results

    return run


CHECKS: Mapping[str, Callable[[ValidateContext], list[CheckResult]]] = {
    code: _for_each_rule(code, check) for code, check in _RULE_CHECKS.items()
}


def run_checks(ctx: ValidateContext) -> list[CheckResult]:
    """Run every validate check and return all results."""
    return [result for check in CHECKS.values() for result in check(ctx)]


# ---- table versions (no-op proof) -------------------------------------------------------


def data_tables() -> list[tuple[str, str]]:
    """Every bronze, silver (with quarantine) and gold table, with its layer."""
    contracts = load_contracts()
    bronze = sorted({c.bronze_table for c in contracts if c.bronze_table})
    silver = sorted(silver_table_of(c) for c in silver_contracts())
    return [
        *[(f"{BRONZE}.{t}", "bronze") for t in bronze],
        *[(f"{SILVER}.{t}{s}", "silver") for t in silver for s in ("", QUARANTINE_SUFFIX)],
        *[(f"{GOLD}.{t}", "gold") for t in GOLD_BUILDERS],
    ]


def content_signature(df: DataFrame) -> tuple[int, Decimal]:
    """Row count and an order-independent checksum: the sum of every row's xxhash64."""
    row_hash = F.xxhash64(*[F.col(c) for c in df.columns]).cast("decimal(38,0)")
    row = df.agg(F.count(F.lit(1)), F.sum(row_hash)).first()
    if row is None:
        return 0, Decimal(0)
    return int(row[0]), Decimal(row[1] or 0)


def compare_to_previous(
    current: Sequence[TableVersion], previous: Mapping[str, TableVersion]
) -> list[TableVersion]:
    """Mark a table changed only if its Delta version moved and its content differs.

    The content check is the fallback for a refresh that writes a new version with identical
    rows. When either version is unknown (a materialized view), content alone decides. A table
    seen for the first time counts as changed.
    """
    marked = []
    for version in current:
        before = previous.get(version.table_name)
        changed = before is None or (
            (
                version.delta_version is None
                or before.delta_version is None
                or version.delta_version != before.delta_version
            )
            and (version.row_count, version.content_checksum)
            != (before.row_count, before.content_checksum)
        )
        marked.append(replace(version, changed_since_previous=changed))
    return marked


# Bronze streaming tables answer DESCRIBE HISTORY. Silver and gold materialized views refuse
# it (EXPECT_TABLE_NOT_VIEW), so compare_to_previous judges them on content alone.
HISTORY_LAYERS = frozenset({"bronze"})


def _latest_version(ctx: ValidateContext, table: str) -> int | None:
    history = ctx.spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()
    return int(history["version"]) if history else None


def capture_table_versions(ctx: ValidateContext) -> list[TableVersion]:
    """Capture each table's content (and bronze's Delta version), compared with the last run."""
    captured_at = ctx.now()
    current = []
    for table, layer in data_tables():
        rows, checksum = content_signature(ctx.spark.table(table))
        current.append(
            TableVersion(
                run_id=ctx.run_id,
                table_name=table,
                layer=layer,
                delta_version=_latest_version(ctx, table) if layer in HISTORY_LAYERS else None,
                row_count=rows,
                content_checksum=checksum,
                changed_since_previous=False,
                captured_at=captured_at,
            )
        )
    return compare_to_previous(current, _previous_versions(ctx))


def _previous_versions(ctx: ValidateContext) -> dict[str, TableVersion]:
    if not ctx.spark.catalog.tableExists(f"{OPS}.table_versions"):
        return {}
    rows = ctx.spark.sql(
        f"""
        SELECT * FROM (
          SELECT *, row_number() OVER (PARTITION BY table_name ORDER BY captured_at DESC) AS rn
          FROM {OPS}.table_versions WHERE run_id <> :run_id
        ) WHERE rn = 1
        """,
        args={"run_id": ctx.run_id},
    ).collect()
    return {
        row["table_name"]: TableVersion(**{k: v for k, v in row.asDict().items() if k != "rn"})
        for row in rows
    }
