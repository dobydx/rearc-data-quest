import json
import math
import re
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession

from quest.gold.pyspark_gold import (
    GOLD_BUILDERS,
    build_gold,
    population_stats,
    prs30006032_q01_with_population,
    series_best_year,
)
from quest.gold.sql_bodies import load_gold_selects
from quest.validate.checks import gold_mismatches
from tests.spark.conftest import GOLD_SQL, Silver, save

pytestmark = pytest.mark.spark

# Minimal silver shapes for the connect_safe twins: only the columns the builders read.
OBSERVATIONS_DDL = (
    "series_id STRING, year INT, period STRING, value DECIMAL(12,3), is_revised BOOLEAN"
)
SERIES_DDL = (
    "series_id STRING, series_label STRING, sector_name STRING, measure_text STRING, "
    "duration_text STRING, unit_of_measure STRING"
)

GOLD_TABLES = (
    "population_stats_2013_2018",
    "series_best_year",
    "prs30006032_q01_with_population",
    "productivity_observations",
)


@pytest.fixture(scope="module")
def selects() -> dict[str, str]:
    return load_gold_selects(GOLD_SQL)


def sql_gold(spark: SparkSession, selects: dict[str, str], name: str) -> DataFrame:
    return spark.sql(selects[name])


def pyspark_gold(spark: SparkSession, name: str) -> DataFrame:
    return build_gold(name, lambda table: spark.table(f"quest_silver.{table}"))


def test_sql_file_defines_the_four_gold_tables(selects: dict[str, str]) -> None:
    assert set(selects) == set(GOLD_TABLES)
    assert set(GOLD_BUILDERS) == set(GOLD_TABLES)


def test_sql_reads_two_part_silver_names_only(selects: dict[str, str]) -> None:
    for name, body in selects.items():
        sources = set(re.findall(r"\b(?:FROM|JOIN)\s+([\w.]+)", body))
        assert sources, name
        assert all(s.startswith("quest_silver.") or "." not in s for s in sources), (name, sources)


def test_q1_values(spark: SparkSession, silver_catalog: Silver, selects: dict[str, str]) -> None:
    (row,) = sql_gold(spark, selects, "population_stats_2013_2018").collect()

    assert (row["year_from"], row["year_to"], row["n_years"], row["is_complete"]) == (
        2013,
        2018,
        6,
        True,
    )
    assert row["population_mean"] == 322069808.0
    assert math.isclose(row["population_stddev_samp"], 4158441.040908095, rel_tol=1e-9)
    assert math.isclose(row["population_stddev_pop"], 3796119.936934378, rel_tol=1e-9)
    assert (row["population_min"], row["population_max"]) == (316128839, 327167439)


@pytest.mark.connect_safe
def test_pyspark_q1_gives_the_golden_numbers_on_dataframes(
    spark: SparkSession, fixtures_dir: Path
) -> None:
    # Built from createDataFrame, so it also runs on serverless; the SQL twin needs tables.
    records = json.loads((fixtures_dir / "datausa_population.json").read_bytes())["data"]
    silver = spark.createDataFrame(
        [(r["Year"], int(r["Population"])) for r in records], "year INT, population BIGINT"
    )

    (row,) = population_stats(silver).collect()

    assert (row["n_years"], row["is_complete"], row["population_mean"]) == (6, True, 322069808.0)
    assert (row["population_stddev_samp"], row["population_stddev_pop"]) == (
        4158441.040908095,
        3796119.936934378,
    )
    assert (row["population_min"], row["population_max"]) == (316128839, 327167439)


def _series(spark: SparkSession, *series_ids: str) -> DataFrame:
    return spark.createDataFrame(
        [(sid, f"label {sid}", "sector", "measure", "duration", "INDEX") for sid in series_ids],
        SERIES_DDL,
    )


@pytest.mark.connect_safe
def test_pyspark_q2_on_dataframes_handles_ties_partial_years_and_annual_only(
    spark: SparkSession,
) -> None:
    quarters = ("Q01", "Q02", "Q03", "Q04")
    values = [
        # TIE: 2000 and 2001 both sum to 4; the earliest year wins. Q05 must not be added.
        *[("TIE", 2000, q, "1") for q in quarters],
        ("TIE", 2000, "Q05", "100"),
        *[("TIE", 2001, q, v) for q, v in zip(quarters, ("2", "1", "1", "0"), strict=True)],
        # PARTIAL: three quarters of 1990 beat all four of 1991.
        *[("PARTIAL", 1990, q, "5") for q in quarters[:3]],
        *[("PARTIAL", 1991, q, "1") for q in quarters],
        # ANNUAL: only a Q05 annual average, so no best year.
        ("ANNUAL", 1995, "Q05", "50"),
    ]
    observations = spark.createDataFrame(
        [(sid, year, period, Decimal(v), False) for sid, year, period, v in values],
        OBSERVATIONS_DDL,
    )

    rows = {
        r["series_id"]: r
        for r in series_best_year(
            observations, _series(spark, "TIE", "PARTIAL", "ANNUAL")
        ).collect()
    }

    tie, partial, annual = rows["TIE"], rows["PARTIAL"], rows["ANNUAL"]
    assert (tie["best_year"], str(tie["best_year_value_sum"]), tie["tied_year_count"]) == (
        2000,
        "4.000",
        2,
    )
    assert (partial["best_year"], partial["quarters_in_best_year"], partial["is_partial_year"]) == (
        1990,
        3,
        True,
    )
    assert (annual["has_quarterly_data"], annual["best_year"]) == (False, None)


@pytest.mark.connect_safe
def test_pyspark_q3_on_dataframes_left_joins_population(spark: SparkSession) -> None:
    observations = spark.createDataFrame(
        [
            ("PRS30006032", 2019, "Q01", Decimal("1.500"), False),
            ("PRS30006032", 2020, "Q01", Decimal("-2.000"), True),
            ("PRS30006032", 2019, "Q02", Decimal("9.000"), False),
            ("PRS84006021", 2019, "Q01", Decimal("9.000"), False),
        ],
        OBSERVATIONS_DDL,
    )
    population = spark.createDataFrame([(2019, 328239523)], "year INT, population BIGINT")

    rows = {
        r["year"]: r
        for r in prs30006032_q01_with_population(
            observations, _series(spark, "PRS30006032", "PRS84006021"), population
        ).collect()
    }

    assert sorted(rows) == [2019, 2020]
    assert (rows[2019]["population"], rows[2019]["has_population"]) == (328239523, True)
    assert (rows[2020]["population"], rows[2020]["has_population"]) == (None, False)
    assert (str(rows[2020]["value"]), rows[2020]["is_revised"]) == ("-2.000", True)


def test_q1_is_bit_identical_for_any_row_order(
    spark: SparkSession, silver_catalog: Silver, selects: dict[str, str]
) -> None:
    # Spark's stddev adds doubles in row order, so a recompute with the same rows could move
    # the last digits and fail the no-op proof (workspace finding, 2026-11-03).
    table = "quest_silver.datausa_population"
    original = spark.table(table)
    rows, schema = original.collect(), original.schema
    seen = set()
    try:
        for order, parts in ((rows, 1), (rows[::-1], 1), (rows[3:] + rows[:3], 3), (rows, 6)):
            save(spark, spark.createDataFrame(order, schema).repartition(parts), table)
            for q1 in (
                sql_gold(spark, selects, GOLD_TABLES[0]),
                pyspark_gold(spark, GOLD_TABLES[0]),
            ):
                (row,) = q1.collect()
                seen.add((row["population_stddev_samp"], row["population_stddev_pop"]))
    finally:
        save(spark, spark.createDataFrame(rows, schema), table)

    # Python's statistics.stdev and pstdev give the correctly rounded values.
    assert seen == {(4158441.040908095, 3796119.936934378)}


def test_q2_values_on_slice(
    spark: SparkSession, silver_catalog: Silver, selects: dict[str, str]
) -> None:
    rows = {r["series_id"]: r for r in sql_gold(spark, selects, "series_best_year").collect()}

    assert len(rows) == 6
    expected = {
        "PRS30006032": (1994, "14.600"),
        "PRS30006013": (1989, "578.369"),
        "PRS84006021": (1964, "4.900"),
        "PRS84006102": (1974, "44.100"),
    }
    for series_id, (year, total) in expected.items():
        assert (rows[series_id]["best_year"], str(rows[series_id]["best_year_value_sum"])) == (
            year,
            total,
        )
    assert rows["PRS84006021"]["tied_year_count"] == 2
    assert rows["PRS84006102"]["tied_year_count"] == 2
    assert rows["PRS30006032"]["tied_year_count"] == 1
    partial = rows["PRS32006212"]
    assert (partial["best_year"], partial["quarters_in_best_year"], partial["is_partial_year"]) == (
        1987,
        3,
        True,
    )
    annual = rows["PRS30006081"]
    assert (annual["best_year"], annual["has_quarterly_data"]) == (None, False)
    assert rows["PRS30006013"]["has_quarterly_data"] is True


def test_q3_rows(spark: SparkSession, silver_catalog: Silver, selects: dict[str, str]) -> None:
    rows = {
        r["year"]: r for r in sql_gold(spark, selects, "prs30006032_q01_with_population").collect()
    }

    assert len(rows) == 39
    assert (min(rows), max(rows)) == (1988, 2026)
    assert str(rows[1988]["value"]) == "2.100"
    assert str(rows[2026]["value"]) == "0.000"
    assert sum(1 for r in rows.values() if r["has_population"]) == 11
    assert rows[2020]["population"] is None
    assert rows[2013]["population"] == 316128839


def test_productivity_observations_matches_silver_grain(
    spark: SparkSession, silver_catalog: Silver, selects: dict[str, str]
) -> None:
    df = sql_gold(spark, selects, "productivity_observations")

    assert df.count() == 1409
    assert df.where("series_label IS NULL OR period_name IS NULL").count() == 0


@pytest.mark.parametrize("name", GOLD_TABLES)
def test_sql_and_pyspark_identical(
    spark: SparkSession, silver_catalog: Silver, selects: dict[str, str], name: str
) -> None:
    sql_df = sql_gold(spark, selects, name)
    py_df = pyspark_gold(spark, name)
    columns = sorted(sql_df.columns)
    assert sorted(py_df.columns) == columns
    sql_df, py_df = sql_df.select(*columns), py_df.select(*columns)
    assert sql_df.dtypes == py_df.dtypes

    assert gold_mismatches(sql_df, py_df, rel_tol=1e-9) == 0
    assert sql_df.count() == py_df.count()


@pytest.mark.parametrize("name", GOLD_TABLES)
def test_declared_columns_match_query_output(
    spark: SparkSession, silver_catalog: Silver, selects: dict[str, str], name: str
) -> None:
    ddl = GOLD_SQL.read_text().split(f"MATERIALIZED VIEW quest_gold.{name} (", 1)[1]
    column_block = ddl.split("CONSTRAINT", 1)[0]
    declared = re.findall(r"^\s+(\w+) ([A-Z]+(?:\([0-9,]+\))?(?:<[A-Z]+>)?)", column_block, re.M)

    assert [(column, sql_type.lower()) for column, sql_type in declared] == (
        sql_gold(spark, selects, name).dtypes
    )
