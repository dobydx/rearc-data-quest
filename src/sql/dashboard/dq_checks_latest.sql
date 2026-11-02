-- Every check of the latest validate run, problems first.
WITH latest AS (
  SELECT max_by(run_id, checked_at) AS run_id FROM quest_ops.dq_checks
)
SELECT
  c.run_id,
  c.check_code,
  c.table_name,
  c.layer,
  c.severity,
  c.status,
  c.observed_value,
  c.threshold,
  c.failing_rows,
  c.details,
  c.checked_at
FROM quest_ops.dq_checks AS c
JOIN latest AS l
  ON l.run_id = c.run_id
ORDER BY
  CASE c.status WHEN 'error' THEN 0 WHEN 'fail' THEN 1 WHEN 'warn' THEN 2 ELSE 3 END,
  c.check_code,
  c.table_name
