-- quest_dq_alert: one row; the alert fires when issues > 0.
-- Counts the warn, fail and error checks of the latest validate run, plus 1 when the served
-- pr.data.1.AllData is more than 100 days past its Last-Modified (same bound as STALE_SOURCE)
-- or has no state row or Last-Modified at all. Freshness is computed live, from file_state.
WITH latest AS (
  SELECT max_by(run_id, checked_at) AS run_id FROM quest_ops.dq_checks
),
problems AS (
  SELECT count(*) AS n
  FROM quest_ops.dq_checks AS c
  JOIN latest AS l
    ON l.run_id = c.run_id
  WHERE c.status IN ('warn', 'fail', 'error')
),
freshness AS (
  SELECT max(datediff(current_date(), http_last_modified)) AS age_days
  FROM quest_ops.file_state
  WHERE file_name = 'pr.data.1.AllData'
)
SELECT
  p.n + CASE WHEN f.age_days > 100 OR f.age_days IS NULL THEN 1 ELSE 0 END AS issues,
  p.n AS problem_checks,
  f.age_days AS alldata_age_days
FROM problems AS p
CROSS JOIN freshness AS f
