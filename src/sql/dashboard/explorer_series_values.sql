-- Explorer line chart: one series, picked by the sector, measure and duration filters.
-- Quarterly rows only: Q05 is the annual value and would plot as a fifth quarter.
SELECT
  concat(series_id, ', ', unit_of_measure) AS series,
  series_id,
  series_label,
  unit_of_measure,
  period_start_date,
  year,
  period,
  value,
  is_revised
FROM quest_gold.productivity_observations
WHERE sector_name = :sector
  AND measure_text = :measure
  AND duration_text = :duration
  AND NOT is_annual_average
ORDER BY period_start_date
