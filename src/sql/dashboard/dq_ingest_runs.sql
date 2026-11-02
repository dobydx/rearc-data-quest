-- Runs timeline: one row per ingest run with its duration and counts.
SELECT
  run_id,
  started_at,
  status,
  trigger,
  code_version,
  timestampdiff(SECOND, started_at, ended_at) AS duration_s,
  versions_landed,
  pointers_flipped,
  files_skipped_fingerprint,
  files_failed,
  error_message
FROM quest_ops.ingest_runs
ORDER BY started_at DESC
