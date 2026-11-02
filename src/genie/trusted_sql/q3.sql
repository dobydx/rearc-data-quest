-- Q3 answer as published by the gold view (built by gold.sql).
SELECT
  series_id,
  series_label,
  year,
  period,
  value,
  unit_of_measure,
  is_revised,
  population,
  has_population
FROM quest_gold.prs30006032_q01_with_population
ORDER BY year
