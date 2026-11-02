-- Expectation pass rates of the latest pipeline update that reported any, from the published
-- event log. A no-op update recomputes nothing and reports no expectations.
WITH progress AS (
  SELECT
    origin.update_id AS update_id,
    timestamp,
    details:flow_progress:data_quality:expectations AS expectations
  FROM quest_ops.pipeline_event_log
  WHERE event_type = 'flow_progress'
    AND details:flow_progress:data_quality:expectations IS NOT NULL
),
latest AS (
  SELECT max_by(update_id, timestamp) AS update_id FROM progress
),
results AS (
  SELECT explode(from_json(
    p.expectations,
    'array<struct<name: STRING, dataset: STRING, passed_records: BIGINT, failed_records: BIGINT>>'
  )) AS e
  FROM progress AS p
  JOIN latest AS l
    ON l.update_id = p.update_id
)
SELECT
  e.dataset AS dataset,
  e.name AS expectation,
  sum(e.passed_records) AS passed_records,
  sum(e.failed_records) AS failed_records,
  try_divide(sum(e.passed_records), sum(e.passed_records) + sum(e.failed_records)) AS pass_rate
FROM results
GROUP BY e.dataset, e.name
ORDER BY pass_rate, dataset, expectation
