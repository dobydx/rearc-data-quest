from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from quest.contracts.loader import contract_by_role, load_rules, rules_for
from quest.contracts.models import FileContract, Rule
from quest.transforms.bronze import with_lineage
from quest.transforms.dq import (
    expectations,
    quarantine_rules,
    quarantined_rows,
    trim_columns,
    valid_rows,
    with_dq_reasons,
)
from tests.spark.conftest import read_landed

pytestmark = pytest.mark.spark

NOT_NULL = Rule("NOT_NULL", "t", "x IS NOT NULL", "quarantine", "row", "")
POSITIVE = Rule("POSITIVE", "t", "x > 0", "quarantine", "row", "")
SHORT = Rule("SHORT", "t", "length(y) < 3", "quarantine", "row", "")

# Helper columns the silver flaggers add, with a passing value, per silver table.
OBSERVATION_HELPERS: dict[str, object] = {
    "_series_exists": True,
    "_period_exists": True,
    "_key_count": 1,
    "_unit_of_measure": "INDEX",
    "_footnotes_known": True,
    "_max_year": 2026,
    "_rescued_data": None,
}
HELPERS: dict[str, dict[str, object]] = {
    "bls_pr_observations": OBSERVATION_HELPERS,
    "bls_pr_series": {
        "_key_count": 1,
        "_unit_of_measure": "INDEX",
        **{f"_{n}_exists": True for n in ("sector", "class", "measure", "seasonal", "duration")},
        "_rescued_data": None,
    },
    "datausa_population": {"_key_count": 1, "_max_year": 2026, "_expected_nation_id": "01000US"},
}


def with_helpers(df: DataFrame, values: dict[str, object]) -> DataFrame:
    return df.withColumns({name: F.lit(value) for name, value in values.items()})


def _frame(spark: SparkSession, rows: list[tuple[int | None, str | None]]) -> DataFrame:
    return spark.createDataFrame(rows, "x INT, y STRING")


@pytest.mark.connect_safe
def test_null_predicate_counts_as_failure(spark: SparkSession) -> None:
    flagged = with_dq_reasons(_frame(spark, [(None, "a")]), [POSITIVE])

    assert flagged.first()["dq_reasons"] == ["POSITIVE"]  # type: ignore[index]


@pytest.mark.connect_safe
def test_valid_and_quarantine_are_complements(spark: SparkSession) -> None:
    df = _frame(spark, [(1, "a"), (-1, "b"), (None, "c"), (5, "long")])
    flagged = with_dq_reasons(df, [NOT_NULL, POSITIVE, SHORT])

    valid = flagged.where(F.size("dq_reasons") == 0)
    rejected = flagged.where(F.size("dq_reasons") > 0)

    assert valid.count() + rejected.count() == df.count()
    assert valid.join(rejected, "y").count() == 0
    assert [r["y"] for r in valid.collect()] == ["a"]


@pytest.mark.connect_safe
def test_multiple_reasons_collected(spark: SparkSession) -> None:
    flagged = with_dq_reasons(_frame(spark, [(None, "long")]), [NOT_NULL, POSITIVE, SHORT])

    assert flagged.first()["dq_reasons"] == ["NOT_NULL", "POSITIVE", "SHORT"]  # type: ignore[index]


@pytest.mark.connect_safe
def test_no_rules_means_empty_reasons(spark: SparkSession) -> None:
    flagged = with_dq_reasons(_frame(spark, [(1, "a")]), [])

    assert flagged.first()["dq_reasons"] == []  # type: ignore[index]
    assert dict(flagged.dtypes)["dq_reasons"] == "array<string>"


@pytest.mark.connect_safe
def test_valid_rows_drop_helpers_and_keep_lineage(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(1, True, "f", "s")], "x INT, _helper BOOLEAN, _file_name STRING, _sha256 STRING"
    )

    valid = valid_rows(with_dq_reasons(df, [POSITIVE]))

    assert valid.columns == ["x", "_file_name", "_sha256"]


def test_trim_keeps_raw_for_quarantine(spark: SparkSession, landed_dir: Path) -> None:
    contract = contract_by_role("observations")
    bronze = with_lineage(read_landed(spark, landed_dir, contract))

    trimmed = trim_columns(bronze, contract).first()

    assert trimmed is not None
    assert trimmed["series_id"] == trimmed["series_id"].strip()
    assert trimmed["_raw_series_id"].endswith(" ")


def test_short_line_is_quarantined_not_dropped(spark: SparkSession, tmp_path: Path) -> None:
    contract = contract_by_role("observations")
    sha = "c" * 64
    path = tmp_path / "bls_pr" / "pr.data.1.AllData" / f"sha256={sha}" / "pr.data.1.AllData"
    path.parent.mkdir(parents=True)
    path.write_bytes(
        b"series_id\tyear\tperiod\tvalue\tfootnote_codes\r\n"
        b"PRS30006032      \t2020\tQ01\t       1.0\t\r\n"
        b"PRS30006032      \t2020\tQ02\t\r\n"
    )
    bronze = with_lineage(read_landed(spark, tmp_path, contract))
    flagged = with_dq_reasons(
        with_helpers(trim_columns(bronze, contract), OBSERVATION_HELPERS),
        quarantine_rules("bls_pr_observations"),
    )

    rejected = quarantined_rows(flagged, contract).collect()

    assert valid_rows(flagged).count() == 1
    assert len(rejected) == 1
    assert "BAD_VALUE" in rejected[0]["dq_reasons"]
    assert rejected[0]["series_id"] == "PRS30006032      "
    assert rejected[0]["_sha256"] == sha
    assert set(quarantined_rows(flagged, contract).columns) == {
        *contract.column_names,
        "dq_reasons",
        "_file_name",
        "_sha256",
        "_source_path",
        "_rescued_data",
    }


@pytest.mark.connect_safe
@pytest.mark.parametrize("role", ["observations", "series", "population", "measure", "period"])
def test_every_row_predicate_evaluates(spark: SparkSession, role: str) -> None:
    contract = contract_by_role(role)
    table = contract.silver_table
    assert table is not None
    df = spark.createDataFrame([tuple("" for _ in contract.column_names)], bronze_ddl(contract))
    df = with_helpers(df, HELPERS.get(table, {"_key_count": 1, "_rescued_data": None}))

    flagged = with_dq_reasons(df, rules_for(table, "row"))

    assert flagged.first() is not None


@pytest.mark.connect_safe
def test_every_pipeline_predicate_evaluates(spark: SparkSession) -> None:
    pipeline_rules = [r for r in load_rules() if r.stage == "pipeline"]

    flagged = with_dq_reasons(spark.createDataFrame([(3,)], "row_count BIGINT"), pipeline_rules)

    assert flagged.first()["dq_reasons"] == []  # type: ignore[index]


def test_quarantine_rules_exclude_warn_rules() -> None:
    codes = {rule.code for rule in quarantine_rules("bls_pr_observations")}

    assert "UNKNOWN_FOOTNOTE" not in codes
    assert "BAD_VALUE" in codes


def test_expectations_map_code_to_predicate() -> None:
    rules = rules_for("bls_pr_observations", "row")

    expected = expectations(rules)

    assert expected["BAD_PERIOD"] == "period IN ('Q01', 'Q02', 'Q03', 'Q04', 'Q05')"
    assert len(expected) == len(rules)


def bronze_ddl(contract: FileContract) -> str:
    return ", ".join(f"{name} STRING" for name in contract.column_names)


def test_rescued_data_row_is_quarantined(spark: SparkSession, landed_dir: Path) -> None:
    contract = contract_by_role("observations")
    bronze = with_lineage(read_landed(spark, landed_dir, contract)).limit(2)
    rescued = bronze.withColumn(
        "_rescued_data",
        F.when(F.col("period") == F.lit("Q01"), F.lit('{"_c5":"extra"}')),
    )
    flagged = with_dq_reasons(
        with_helpers(
            trim_columns(rescued, contract),
            {k: v for k, v in OBSERVATION_HELPERS.items() if k != "_rescued_data"},
        ),
        quarantine_rules("bls_pr_observations"),
    )

    (row,) = quarantined_rows(flagged, contract).collect()

    assert row["dq_reasons"] == ["RESCUED_DATA"]
    assert row["_rescued_data"] == '{"_c5":"extra"}'
