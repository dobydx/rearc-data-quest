-- Quarantined rows by silver table and reason. Every silver table is listed, so a 0 is shown
-- rather than an empty chart. A row with two reasons counts once per reason.
WITH quarantine AS (
  SELECT 'bls_pr_observations' AS table_name, dq_reasons FROM quest_silver.bls_pr_observations_quarantine
  UNION ALL SELECT 'bls_pr_series', dq_reasons FROM quest_silver.bls_pr_series_quarantine
  UNION ALL SELECT 'bls_pr_class', dq_reasons FROM quest_silver.bls_pr_class_quarantine
  UNION ALL SELECT 'bls_pr_duration', dq_reasons FROM quest_silver.bls_pr_duration_quarantine
  UNION ALL SELECT 'bls_pr_footnote', dq_reasons FROM quest_silver.bls_pr_footnote_quarantine
  UNION ALL SELECT 'bls_pr_measure', dq_reasons FROM quest_silver.bls_pr_measure_quarantine
  UNION ALL SELECT 'bls_pr_period', dq_reasons FROM quest_silver.bls_pr_period_quarantine
  UNION ALL SELECT 'bls_pr_seasonal', dq_reasons FROM quest_silver.bls_pr_seasonal_quarantine
  UNION ALL SELECT 'bls_pr_sector', dq_reasons FROM quest_silver.bls_pr_sector_quarantine
  UNION ALL SELECT 'datausa_population', dq_reasons FROM quest_silver.datausa_population_quarantine
),
reasons AS (
  SELECT table_name, explode(dq_reasons) AS reason FROM quarantine
),
tables AS (
  SELECT explode(array(
    'bls_pr_observations', 'bls_pr_series', 'bls_pr_class', 'bls_pr_duration', 'bls_pr_footnote',
    'bls_pr_measure', 'bls_pr_period', 'bls_pr_seasonal', 'bls_pr_sector', 'datausa_population'
  )) AS table_name
)
SELECT
  t.table_name,
  coalesce(r.reason, '(none)') AS reason,
  count(r.reason) AS quarantined_rows
FROM tables AS t
LEFT JOIN reasons AS r
  ON r.table_name = t.table_name
GROUP BY t.table_name, r.reason
ORDER BY quarantined_rows DESC, t.table_name
