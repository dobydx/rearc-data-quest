-- Gold answers. SQL is the primary implementation; quest.gold.pyspark_gold holds the PySpark
-- twins, and tests/spark/test_gold.py proves both return identical rows.
-- quest.gold.sql_bodies relies on one layout: each query starts after a line that is exactly
-- "AS" and ends at the first line ending in ";".

-- Q1: mean and standard deviation of US population, 2013 to 2018.
CREATE OR REFRESH MATERIALIZED VIEW quest_gold.population_stats_2013_2018 (
  year_from INT NOT NULL COMMENT 'First year of the window.',
  year_to INT NOT NULL COMMENT 'Last year of the window.',
  n_years BIGINT NOT NULL COMMENT 'Years in the window that have a population value.',
  is_complete BOOLEAN NOT NULL COMMENT 'True when every year of the window has a value.',
  population_mean DOUBLE COMMENT 'Mean US population over the window, unrounded.',
  population_stddev_samp DOUBLE COMMENT 'Sample standard deviation (n - 1), the quest answer.',
  population_stddev_pop DOUBLE COMMENT 'Population standard deviation (n), for comparison.',
  population_min BIGINT COMMENT 'Smallest yearly population in the window.',
  population_max BIGINT COMMENT 'Largest yearly population in the window.',
  CONSTRAINT population_stats_2013_2018_pk PRIMARY KEY (year_from, year_to) NORELY
)
COMMENT 'Q1. One row: US population statistics for 2013 to 2018 from DataUSA (ACS 1-year).'
TBLPROPERTIES ('quest.layer' = 'gold', 'quest.code_version' = '${quest.code_version}')
AS
-- Moments come from exact DECIMAL sums, then one DOUBLE sqrt. stddev_samp() adds doubles in
-- row order, so its last digits can move when a refresh reads the same rows in another order.
WITH sums AS (
  SELECT
    count(*) AS n,
    sum(CAST(population AS DECIMAL(38,0))) AS sum_x,
    sum(CAST(population AS DECIMAL(38,0)) * CAST(population AS DECIMAL(38,0))) AS sum_xx,
    min(population) AS population_min,
    max(population) AS population_max
  FROM quest_silver.datausa_population
  WHERE year BETWEEN 2013 AND 2018
)
SELECT
  2013 AS year_from,
  2018 AS year_to,
  n AS n_years,
  n = 2018 - 2013 + 1 AS is_complete,
  CASE WHEN n > 0 THEN CAST(sum_x AS DOUBLE) / n END AS population_mean,
  CASE WHEN n > 1 THEN sqrt(CAST(n * sum_xx - sum_x * sum_x AS DOUBLE) / (n * (n - 1))) END
    AS population_stddev_samp,
  CASE WHEN n > 0 THEN sqrt(CAST(n * sum_xx - sum_x * sum_x AS DOUBLE) / (n * n)) END
    AS population_stddev_pop,
  population_min,
  population_max
FROM sums;

-- Q2: for every series, the year with the largest sum of quarterly values.
CREATE OR REFRESH MATERIALIZED VIEW quest_gold.series_best_year (
  series_id STRING NOT NULL COMMENT 'BLS series id.',
  series_label STRING NOT NULL COMMENT 'Measure | sector | class | duration | seasonal adjustment.',
  sector_name STRING NOT NULL COMMENT 'Sector name.',
  measure_text STRING NOT NULL COMMENT 'Measure name.',
  duration_text STRING NOT NULL COMMENT 'Duration name; says whether values are % changes or an index.',
  unit_of_measure STRING NOT NULL COMMENT 'PERCENT_CHANGE or INDEX. A sum of % changes has no economic meaning.',
  has_quarterly_data BOOLEAN NOT NULL COMMENT 'False for annual-only series, which have no best year.',
  best_year INT COMMENT 'Year with the largest sum of Q01 to Q04 values; the earliest year wins a tie.',
  best_year_value_sum DECIMAL(22,3) COMMENT 'Sum of Q01 to Q04 values in the best year. Q05 is excluded.',
  quarters_in_best_year INT COMMENT 'Quarters summed in the best year (1 to 4).',
  is_partial_year BOOLEAN COMMENT 'True when the best year has fewer than 4 quarters.',
  tied_year_count INT COMMENT 'Years that share the largest sum (1 when there is no tie).',
  years_with_quarterly_data INT COMMENT 'Years with at least one quarterly value.',
  CONSTRAINT series_best_year_pk PRIMARY KEY (series_id) NORELY
)
COMMENT 'Q2. One row per series in bls_pr_series: its best year by summed quarterly value.'
TBLPROPERTIES ('quest.layer' = 'gold', 'quest.code_version' = '${quest.code_version}')
AS
WITH yearly AS (
  SELECT series_id, year, sum(value) AS value_sum, count(*) AS quarters
  FROM quest_silver.bls_pr_observations
  WHERE period IN ('Q01', 'Q02', 'Q03', 'Q04')
  GROUP BY series_id, year
),
ranked AS (
  SELECT
    series_id,
    year,
    value_sum,
    quarters,
    row_number() OVER (PARTITION BY series_id ORDER BY value_sum DESC, year ASC) AS year_rank,
    count(*) OVER (PARTITION BY series_id, value_sum) AS tied_year_count,
    count(*) OVER (PARTITION BY series_id) AS years_with_quarterly_data
  FROM yearly
)
SELECT
  s.series_id,
  s.series_label,
  s.sector_name,
  s.measure_text,
  s.duration_text,
  s.unit_of_measure,
  r.series_id IS NOT NULL AS has_quarterly_data,
  r.year AS best_year,
  r.value_sum AS best_year_value_sum,
  CAST(r.quarters AS INT) AS quarters_in_best_year,
  r.quarters < 4 AS is_partial_year,
  CAST(r.tied_year_count AS INT) AS tied_year_count,
  CAST(r.years_with_quarterly_data AS INT) AS years_with_quarterly_data
FROM quest_silver.bls_pr_series AS s
LEFT JOIN ranked AS r
  ON r.series_id = s.series_id AND r.year_rank = 1;

-- Q3: PRS30006032 Q01 values with the US population of the same year.
CREATE OR REFRESH MATERIALIZED VIEW quest_gold.prs30006032_q01_with_population (
  series_id STRING NOT NULL COMMENT 'Always PRS30006032.',
  series_label STRING NOT NULL COMMENT 'Measure | sector | class | duration | seasonal adjustment.',
  year INT NOT NULL COMMENT 'Observation year.',
  period STRING NOT NULL COMMENT 'Always Q01.',
  value DECIMAL(12,3) NOT NULL COMMENT 'Published Q01 value.',
  unit_of_measure STRING NOT NULL COMMENT 'PERCENT_CHANGE or INDEX.',
  is_revised BOOLEAN NOT NULL COMMENT 'True when BLS marks the value revised.',
  population BIGINT COMMENT 'US population that year; NULL when DataUSA has no value.',
  has_population BOOLEAN NOT NULL COMMENT 'True when population is present.',
  CONSTRAINT prs30006032_q01_with_population_pk PRIMARY KEY (series_id, year, period) NORELY
)
COMMENT 'Q3. One row per year with a PRS30006032 Q01 value, left joined to US population.'
TBLPROPERTIES ('quest.layer' = 'gold', 'quest.code_version' = '${quest.code_version}')
AS
SELECT
  o.series_id,
  s.series_label,
  o.year,
  o.period,
  o.value,
  s.unit_of_measure,
  o.is_revised,
  p.population,
  p.population IS NOT NULL AS has_population
FROM quest_silver.bls_pr_observations AS o
LEFT JOIN quest_silver.bls_pr_series AS s
  ON s.series_id = o.series_id
LEFT JOIN quest_silver.datausa_population AS p
  ON p.year = o.year
WHERE o.series_id = 'PRS30006032' AND o.period = 'Q01';

-- Denormalized fact for the dashboard and Genie. No population column on purpose: joining an
-- annual value onto quarterly rows invites wrong sums.
CREATE OR REFRESH MATERIALIZED VIEW quest_gold.productivity_observations (
  series_id STRING NOT NULL COMMENT 'BLS series id.',
  series_label STRING NOT NULL COMMENT 'Measure | sector | class | duration | seasonal adjustment.',
  sector_code STRING NOT NULL COMMENT 'Four-digit sector code.',
  sector_name STRING NOT NULL COMMENT 'Sector name.',
  class_text STRING NOT NULL COMMENT 'Worker class.',
  measure_code STRING NOT NULL COMMENT 'Two-digit measure code.',
  measure_text STRING NOT NULL COMMENT 'Measure name.',
  duration_code STRING NOT NULL COMMENT 'Duration code.',
  duration_text STRING NOT NULL COMMENT 'Duration name.',
  unit_of_measure STRING NOT NULL COMMENT 'PERCENT_CHANGE or INDEX. Never sum % changes across periods.',
  change_basis STRING COMMENT 'SAME_QUARTER_PRIOR_YEAR or PREVIOUS_QUARTER; NULL for an index.',
  base_year INT COMMENT 'Index base year (value 100); NULL for % change series.',
  seasonal_text STRING NOT NULL COMMENT 'Seasonal adjustment, canonical text.',
  frequency STRING NOT NULL COMMENT 'QUARTERLY or ANNUAL (annual-only series).',
  year INT NOT NULL COMMENT 'Observation year.',
  period STRING NOT NULL COMMENT 'Q01 to Q04, or Q05 for the BLS annual value.',
  period_name STRING NOT NULL COMMENT 'Period name.',
  quarter_number TINYINT COMMENT '1 to 4; NULL for Q05, the annual value.',
  is_annual_average BOOLEAN NOT NULL COMMENT 'True for Q05 rows, the annual value. Never add them to quarters.',
  period_start_date DATE COMMENT 'First day of the quarter; NULL for Q05.',
  value DECIMAL(12,3) NOT NULL COMMENT 'Published value, in unit_of_measure.',
  is_revised BOOLEAN NOT NULL COMMENT 'True when BLS marks the value revised.',
  footnote_codes ARRAY<STRING> NOT NULL COMMENT 'BLS footnote codes on the value.',
  CONSTRAINT productivity_observations_pk PRIMARY KEY (series_id, year, period) NORELY
)
COMMENT 'One row per series, year and period, with series labels, units and period names.'
TBLPROPERTIES ('quest.layer' = 'gold', 'quest.code_version' = '${quest.code_version}')
AS
SELECT
  o.series_id,
  s.series_label,
  s.sector_code,
  s.sector_name,
  s.class_text,
  s.measure_code,
  s.measure_text,
  s.duration_code,
  s.duration_text,
  s.unit_of_measure,
  s.change_basis,
  s.base_year,
  s.seasonal_text,
  s.frequency,
  o.year,
  o.period,
  p.period_name,
  o.quarter_number,
  o.is_annual_average,
  o.period_start_date,
  o.value,
  o.is_revised,
  o.footnote_codes
FROM quest_silver.bls_pr_observations AS o
LEFT JOIN quest_silver.bls_pr_series AS s
  ON s.series_id = o.series_id
LEFT JOIN quest_silver.bls_pr_period AS p
  ON p.period = o.period;
