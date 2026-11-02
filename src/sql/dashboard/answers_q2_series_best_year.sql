-- Q2 table: best year per series. Sector, unit and partial-year filters sit above it.
SELECT
  series_id,
  series_label,
  sector_name,
  measure_text,
  duration_text,
  unit_of_measure,
  has_quarterly_data,
  best_year,
  best_year_value_sum,
  quarters_in_best_year,
  is_partial_year,
  tied_year_count
FROM quest_gold.series_best_year
ORDER BY series_id
