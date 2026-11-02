-- Filter options for the Explorer: sector, measure and duration pick exactly one series.
SELECT DISTINCT sector_name, measure_text, duration_text
FROM quest_gold.series_best_year
