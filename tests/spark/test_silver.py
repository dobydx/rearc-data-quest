from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from quest.contracts.loader import contract_by_role
from quest.ingest.landing import land
from quest.pipeline_meta import silver_columns
from quest.transforms.bronze import with_lineage
from quest.transforms.dq import quarantined_rows
from quest.transforms.silver import (
    LOOKUPS,
    current_version_rows,
    flag_observations,
    flag_population,
    max_year_by_sha,
    population,
)
from tests.spark.conftest import (
    ServedVersion,
    Silver,
    build_silver,
    landed_versions,
    ops_frames,
    read_landed,
)

pytestmark = pytest.mark.spark


@pytest.fixture
def silver(spark: SparkSession, landed_dir: Path) -> Silver:
    return build_silver(spark, landed_dir)


def _by(df: DataFrame, key: str) -> dict[str, dict[str, object]]:
    return {row[key]: row.asDict() for row in df.collect()}


def _reasons(flagged: DataFrame) -> list[list[str]]:
    return [row["dq_reasons"] for row in flagged.where(F.size("dq_reasons") > 0).collect()]


def _land_second_class_version(root: Path) -> str:
    body = (
        b"class_code\tclass_text\tdisplay_level\tselectable\tsort_sequence\r\n9\tOther\t0\tT\t3\r\n"
    )
    return land(root, "bls_pr", "pr.class", body).sha256


def _class_rows(spark: SparkSession, root: Path, versions: list[ServedVersion]) -> DataFrame:
    file_state, file_versions = ops_frames(spark, versions)
    bronze = with_lineage(read_landed(spark, root, contract_by_role("class")))
    return current_version_rows(bronze, file_state, file_versions, "bls_pr")


def test_only_current_version_rows_used(spark: SparkSession, landed_dir: Path) -> None:
    new_sha = _land_second_class_version(landed_dir)
    versions = [v for v in landed_versions(landed_dir) if v.sha256 != new_sha]

    rows = _class_rows(spark, landed_dir, versions).collect()

    assert sorted(row["class_code"] for row in rows) == ["3", "6"]
    assert {row["_sha256"] for row in rows} != {new_sha}


def test_missing_status_still_served_deleted_not(spark: SparkSession, landed_dir: Path) -> None:
    versions = [v for v in landed_versions(landed_dir) if v.file_name == "pr.class"]

    missing = _class_rows(spark, landed_dir, [replace(v, status="missing") for v in versions])
    deleted = _class_rows(spark, landed_dir, [replace(v, status="deleted") for v in versions])

    assert missing.count() == 2
    assert deleted.count() == 0


def test_pointer_of_another_source_not_served(spark: SparkSession, landed_dir: Path) -> None:
    versions = [v for v in landed_versions(landed_dir) if v.file_name == "pr.class"]

    rows = _class_rows(spark, landed_dir, [replace(v, source="datausa") for v in versions])

    assert rows.count() == 0


def test_year_bound_joins_on_file_and_sha(spark: SparkSession, landed_dir: Path) -> None:
    versions = landed_versions(landed_dir)
    (alldata,) = [v for v in versions if v.file_name == "pr.data.1.AllData"]
    # Same bytes under another name, released in 1950: its year bound must not leak.
    twin = replace(
        alldata, file_name="pr.data.9.Twin", last_modified=datetime(1950, 1, 1, tzinfo=UTC)
    )

    silver = build_silver(spark, landed_dir, [*versions, twin])

    assert silver.observations.count() == 1409
    assert _reasons(silver.observations_flagged) == []


def test_header_bad_version_not_served(spark: SparkSession, landed_dir: Path) -> None:
    versions = [v for v in landed_versions(landed_dir) if v.file_name == "pr.class"]

    rows = _class_rows(spark, landed_dir, [replace(v, header_ok=False) for v in versions])

    assert rows.count() == 0


def test_series_label_and_units(silver: Silver) -> None:
    series = _by(silver.series, "series_id")

    index = series["PRS30006013"]
    assert (index["unit_of_measure"], index["base_year"], index["value_decimals"]) == (
        "INDEX",
        2017,
        3,
    )
    assert index["change_basis"] is None
    pct = series["PRS30006032"]
    assert (pct["unit_of_measure"], pct["change_basis"]) == ("PERCENT_CHANGE", "PREVIOUS_QUARTER")
    assert pct["base_year"] is None
    assert pct["series_label"] == (
        "Hours worked | Manufacturing | All workers | "
        "% Change from previous quarter | Seasonally Adjusted"
    )
    assert len(series) == 6


def test_seasonal_label_canonicalized_and_source_kept(silver: Silver) -> None:
    seasonal = _by(silver.lookups["seasonal"], "seasonal_code")

    assert seasonal["S"]["seasonal_text"] == "Seasonally Adjusted"
    assert seasonal["S"]["seasonal_text_source"] == "Seasonal Adjusted"
    assert seasonal["U"]["seasonal_text"] == seasonal["U"]["seasonal_text_source"]
    series = _by(silver.series, "series_id")["PRS30006013"]
    assert series["seasonal_text_source"] == "Seasonal Adjusted"


def test_annual_only_series_frequency(silver: Silver) -> None:
    series = _by(silver.series, "series_id")

    assert series["PRS30006081"]["frequency"] == "ANNUAL"
    assert series["PRS30006013"]["frequency"] == "QUARTERLY"


def test_period_dim_flags(silver: Silver) -> None:
    period = _by(silver.lookups["period"], "period")

    assert (period["Q02"]["quarter_number"], period["Q02"]["is_annual_average"]) == (2, False)
    assert (period["Q05"]["quarter_number"], period["Q05"]["is_annual_average"]) == (None, True)


def test_observations_typed_and_complete(silver: Silver) -> None:
    obs = silver.observations

    assert obs.count() == 1409
    assert obs.schema["value"].dataType == DecimalType(12, 3)
    assert obs.where("is_revised").count() == 5
    assert _reasons(silver.observations_flagged) == []
    row = obs.where("series_id = 'PRS30006013' AND year = 1987 AND period = 'Q02'").first()
    assert row is not None
    assert str(row["value"]) == "140.570"
    assert row["footnote_codes"] == []
    assert (row["quarter_number"], row["is_annual_average"]) == (2, False)
    assert str(row["period_start_date"]) == "1987-04-01"


def test_annual_observation_has_no_quarter(silver: Silver) -> None:
    row = silver.observations.where("period = 'Q05'").first()

    assert row is not None
    assert (row["quarter_number"], row["period_start_date"], row["is_annual_average"]) == (
        None,
        None,
        True,
    )


def test_current_file_never_feeds_observations(silver: Silver) -> None:
    files = {
        row["_file_name"] for row in silver.observations.select("_file_name").distinct().collect()
    }

    assert files == {"pr.data.1.AllData"}


def test_orphan_series_quarantined(silver: Silver, spark: SparkSession, landed_dir: Path) -> None:
    file_state, file_versions = ops_frames(spark, landed_versions(landed_dir))
    bronze = with_lineage(read_landed(spark, landed_dir, contract_by_role("observations")))
    flagged = flag_observations(
        current_version_rows(bronze, file_state, file_versions, "bls_pr"),
        silver.series.where("series_id <> 'PRS30006032'"),
        silver.lookups["period"],
        silver.lookups["footnote"],
        max_year_by_sha(file_versions),
    )

    rejected = quarantined_rows(flagged, contract_by_role("observations"))

    assert rejected.count() > 0
    assert {r["series_id"].strip() for r in rejected.collect()} == {"PRS30006032"}
    assert all(r["dq_reasons"] == ["ORPHAN_SERIES"] for r in rejected.collect())


def test_duplicate_key_quarantines_all_copies(spark: SparkSession, landed_dir: Path) -> None:
    body = (
        b"class_code\tclass_text\tdisplay_level\tselectable\tsort_sequence\r\n"
        b"3\tEmployees\t0\tT\t2\r\n6\tAll workers\t0\tT\t1\r\n6\tAll workers again\t0\tT\t1\r\n"
    )
    for path in (landed_dir / "bls_pr" / "pr.class").iterdir():
        for child in path.iterdir():
            child.unlink()
        path.rmdir()
    land(landed_dir, "bls_pr", "pr.class", body)

    silver = build_silver(spark, landed_dir)

    assert _reasons(silver.lookup_flags["class"]) == [["LOOKUP_DUPLICATE_CODE"]] * 2
    assert [r["class_code"] for r in silver.lookups["class"].collect()] == ["3"]


def test_population_bigint_and_non_integral_quarantined(
    silver: Silver, spark: SparkSession
) -> None:
    population = _by(silver.population, "year")

    assert len(population) == 11
    assert population[2013]["population"] == 316128839
    assert dict(silver.population.dtypes)["population"] == "bigint"
    assert population[2013]["source_table_id"] == "B01003"

    bad = _population_row(spark, "1.5")

    assert _reasons(flag_population(bad, "01000US")) == [["NON_INTEGRAL_POPULATION"]]


def _population_row(spark: SparkSession, text: str) -> DataFrame:
    return spark.createDataFrame(
        [("01000US", "United States", "2015", text, "1", "ACS", "B01003", "p", "f", "s", None)],
        "nation_id STRING, nation_name STRING, year STRING, population STRING, page_total STRING, "
        "dataset_name STRING, table_id STRING, _source_path STRING, _file_name STRING, "
        "_sha256 STRING, _ingested_at TIMESTAMP",
    ).withColumn("_ingested_at", F.current_timestamp())


# OSS Spark keeps a JSON number's text (316128839.0); the Databricks JSON reader renders it as
# Java double text (3.16128839E8). Workspace finding, 2026-11-03.
@pytest.mark.connect_safe
@pytest.mark.parametrize(
    ("text", "reasons"),
    [
        ("316128839.0", []),
        ("3.16128839E8", []),
        ("3.161288395E8", [["NON_INTEGRAL_POPULATION"]]),
        # A scale-6 check would round these to whole numbers and silver would round them too.
        ("316128839.0000001", [["NON_INTEGRAL_POPULATION"]]),
        ("3.161288390000004E8", [["NON_INTEGRAL_POPULATION"]]),
        ("abc", [["NON_NUMERIC_POPULATION", "NON_INTEGRAL_POPULATION"]]),
    ],
)
def test_population_rules_accept_both_number_renderings(
    spark: SparkSession, text: str, reasons: list[list[str]]
) -> None:
    assert _reasons(flag_population(_population_row(spark, text), "01000US")) == reasons


@pytest.mark.connect_safe
def test_population_casts_double_text_exactly(spark: SparkSession) -> None:
    (row,) = population(_population_row(spark, "3.16128839E8")).collect()

    assert row["population"] == 316128839


def test_population_other_nation_quarantined(silver: Silver) -> None:
    reasons = _reasons(flag_population(silver.population_current, "04000US06"))

    assert reasons == [["BAD_NATION"]] * 11


SILVER_FRAMES = {
    "observations": lambda s: s.observations,
    "series": lambda s: s.series,
    "population": lambda s: s.population,
    **{role: (lambda s, r=role: s.lookups[r]) for role in LOOKUPS},
}


@pytest.mark.parametrize("role", sorted(SILVER_FRAMES))
def test_silver_columns_and_types_match_ddl(silver: Silver, role: str) -> None:
    df = SILVER_FRAMES[role](silver)
    columns = silver_columns(contract_by_role(role))

    assert df.columns == [c.name for c in columns]
    assert [dtype for _, dtype in df.dtypes] == [c.silver_type.lower() for c in columns]
