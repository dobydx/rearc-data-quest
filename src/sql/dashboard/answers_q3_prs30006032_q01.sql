-- Q3: PRS30006032 Q01 value and US population by year. population is NULL where DataUSA has
-- no value (2020, and every year outside the ACS range).
SELECT
  year,
  value,
  unit_of_measure,
  population,
  has_population
FROM quest_gold.prs30006032_q01_with_population
ORDER BY year
