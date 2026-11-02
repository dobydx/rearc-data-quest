"""PySpark twins of the gold SQL. Each returns the same columns, types and rows as its view."""

from collections.abc import Callable, Mapping

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

QUARTERS = ("Q01", "Q02", "Q03", "Q04")
Q3_SERIES_ID = "PRS30006032"
Q3_PERIOD = "Q01"


def population_stats(
    population: DataFrame, year_from: int = 2013, year_to: int = 2018
) -> DataFrame:
    """Q1: one row of population statistics over ``year_from..year_to``, unrounded.

    Moments come from exact DECIMAL sums, then one DOUBLE sqrt, so a refresh that reads the
    same rows in another order returns the same bits (``stddev_samp`` would not).
    """
    x = F.col("population").cast("decimal(38,0)")
    sums = population.where(F.col("year").between(year_from, year_to)).agg(
        F.count(F.lit(1)).alias("n"),
        F.sum(x).alias("sum_x"),
        F.sum(x * x).alias("sum_xx"),
        F.min("population").alias("population_min"),
        F.max("population").alias("population_max"),
    )
    n, sum_x, sum_xx = F.col("n"), F.col("sum_x"), F.col("sum_xx")
    squared_deviations_times_n = (n * sum_xx - sum_x * sum_x).cast("double")
    return sums.select(
        F.lit(year_from).alias("year_from"),
        F.lit(year_to).alias("year_to"),
        n.alias("n_years"),
        (n == year_to - year_from + 1).alias("is_complete"),
        F.when(n > 0, sum_x.cast("double") / n).alias("population_mean"),
        F.when(n > 1, F.sqrt(squared_deviations_times_n / (n * (n - 1)))).alias(
            "population_stddev_samp"
        ),
        F.when(n > 0, F.sqrt(squared_deviations_times_n / (n * n))).alias("population_stddev_pop"),
        "population_min",
        "population_max",
    )


def series_best_year(observations: DataFrame, series: DataFrame) -> DataFrame:
    """Q2: per series, the year with the largest Q01..Q04 sum; the earliest year wins a tie."""
    yearly = (
        observations.where(F.col("period").isin(*QUARTERS))
        .groupBy("series_id", "year")
        .agg(F.sum("value").alias("value_sum"), F.count(F.lit(1)).alias("quarters"))
    )
    by_series = Window.partitionBy("series_id")
    ranked = yearly.select(
        "series_id",
        "year",
        "value_sum",
        "quarters",
        F.row_number()
        .over(by_series.orderBy(F.col("value_sum").desc(), F.col("year").asc()))
        .alias("year_rank"),
        F.count(F.lit(1))
        .over(Window.partitionBy("series_id", "value_sum"))
        .alias("tied_year_count"),
        F.count(F.lit(1)).over(by_series).alias("years_with_quarterly_data"),
    ).where(F.col("year_rank") == 1)
    s, r = series.alias("s"), ranked.alias("r")
    return s.join(r, F.col("r.series_id") == F.col("s.series_id"), "left").select(
        F.col("s.series_id"),
        F.col("s.series_label"),
        F.col("s.sector_name"),
        F.col("s.measure_text"),
        F.col("s.duration_text"),
        F.col("s.unit_of_measure"),
        F.col("r.series_id").isNotNull().alias("has_quarterly_data"),
        F.col("r.year").alias("best_year"),
        F.col("r.value_sum").alias("best_year_value_sum"),
        F.col("r.quarters").cast("int").alias("quarters_in_best_year"),
        (F.col("r.quarters") < 4).alias("is_partial_year"),
        F.col("r.tied_year_count").cast("int").alias("tied_year_count"),
        F.col("r.years_with_quarterly_data").cast("int").alias("years_with_quarterly_data"),
    )


def prs30006032_q01_with_population(
    observations: DataFrame, series: DataFrame, population: DataFrame
) -> DataFrame:
    """Q3: PRS30006032 Q01 values left joined to the population of the same year."""
    o, s, p = observations.alias("o"), series.alias("s"), population.alias("p")
    return (
        o.join(s, F.col("s.series_id") == F.col("o.series_id"), "left")
        .join(p, F.col("p.year") == F.col("o.year"), "left")
        .where((F.col("o.series_id") == Q3_SERIES_ID) & (F.col("o.period") == Q3_PERIOD))
        .select(
            F.col("o.series_id"),
            F.col("s.series_label"),
            F.col("o.year"),
            F.col("o.period"),
            F.col("o.value"),
            F.col("s.unit_of_measure"),
            F.col("o.is_revised"),
            F.col("p.population"),
            F.col("p.population").isNotNull().alias("has_population"),
        )
    )


def productivity_observations(
    observations: DataFrame, series: DataFrame, period: DataFrame
) -> DataFrame:
    """The denormalized fact: every observation with its series labels, units and period name."""
    o, s, p = observations.alias("o"), series.alias("s"), period.alias("p")
    series_columns = (
        "series_label",
        "sector_code",
        "sector_name",
        "class_text",
        "measure_code",
        "measure_text",
        "duration_code",
        "duration_text",
        "unit_of_measure",
        "change_basis",
        "base_year",
        "seasonal_text",
        "frequency",
    )
    return (
        o.join(s, F.col("s.series_id") == F.col("o.series_id"), "left")
        .join(p, F.col("p.period") == F.col("o.period"), "left")
        .select(
            F.col("o.series_id"),
            *[F.col(f"s.{c}") for c in series_columns],
            F.col("o.year"),
            F.col("o.period"),
            F.col("p.period_name"),
            *[
                F.col(f"o.{c}")
                for c in (
                    "quarter_number",
                    "is_annual_average",
                    "period_start_date",
                    "value",
                    "is_revised",
                    "footnote_codes",
                )
            ],
        )
    )


GOLD_BUILDERS: Mapping[str, Callable[..., DataFrame]] = {
    "population_stats_2013_2018": population_stats,
    "series_best_year": series_best_year,
    "prs30006032_q01_with_population": prs30006032_q01_with_population,
    "productivity_observations": productivity_observations,
}

# Silver inputs of each builder, in parameter order.
GOLD_SOURCES: Mapping[str, tuple[str, ...]] = {
    "population_stats_2013_2018": ("datausa_population",),
    "series_best_year": ("bls_pr_observations", "bls_pr_series"),
    "prs30006032_q01_with_population": (
        "bls_pr_observations",
        "bls_pr_series",
        "datausa_population",
    ),
    "productivity_observations": ("bls_pr_observations", "bls_pr_series", "bls_pr_period"),
}


def build_gold(name: str, silver_table: Callable[[str], DataFrame]) -> DataFrame:
    """Build one gold table, fetching each silver input by table name."""
    return GOLD_BUILDERS[name](*[silver_table(table) for table in GOLD_SOURCES[name]])
