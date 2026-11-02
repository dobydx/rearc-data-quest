-- Per-file actions: what each run did with each file (skipped, new version, same hash, ...).
SELECT
  r.started_at AS run_started_at,
  f.run_id,
  f.source,
  f.file_name,
  f.action,
  f.pointer_flipped,
  f.http_status,
  f.bytes,
  f.error
FROM quest_ops.ingest_run_files AS f
JOIN quest_ops.ingest_runs AS r
  ON r.run_id = f.run_id
ORDER BY run_started_at DESC, f.source, f.file_name
