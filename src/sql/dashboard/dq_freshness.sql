-- Freshness per served file: days since the source's Last-Modified and since ingest last
-- checked it. validate warns (STALE_SOURCE) when pr.data.1.AllData passes 100 days.
SELECT
  source,
  file_name,
  status,
  http_last_modified,
  datediff(current_date(), http_last_modified) AS days_since_modified,
  updated_at AS pointer_updated_at,
  datediff(current_date(), updated_at) AS days_since_pointer_update
FROM quest_ops.file_state
ORDER BY days_since_modified DESC NULLS LAST, source, file_name
