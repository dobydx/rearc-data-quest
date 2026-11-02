-- Q1 tiles: mean and sample standard deviation of US population, 2013 to 2018.
SELECT
  year_from,
  year_to,
  n_years,
  is_complete,
  population_mean,
  population_stddev_samp,
  population_stddev_pop,
  population_min,
  population_max
FROM quest_gold.population_stats_2013_2018
