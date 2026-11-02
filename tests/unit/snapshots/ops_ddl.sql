CREATE TABLE IF NOT EXISTS quest_ops.ingest_runs (
  run_id STRING NOT NULL,
  job_id STRING,
  job_run_id STRING,
  started_at TIMESTAMP NOT NULL,
  ended_at TIMESTAMP,
  status STRING NOT NULL,
  code_version STRING NOT NULL,
  trigger STRING NOT NULL,
  files_listed INT NOT NULL,
  files_skipped_fingerprint INT NOT NULL,
  files_not_modified INT NOT NULL,
  files_same_hash INT NOT NULL,
  versions_landed INT NOT NULL,
  pointers_flipped INT NOT NULL,
  files_missing INT NOT NULL,
  files_deleted INT NOT NULL,
  files_self_healed INT NOT NULL,
  files_failed INT NOT NULL,
  error_class STRING,
  error_message STRING,
  params_json STRING NOT NULL,
  CONSTRAINT ingest_runs_pk PRIMARY KEY (run_id)
) COMMENT 'One row per ingest task run with its counts; the per-run audit.';

CREATE TABLE IF NOT EXISTS quest_ops.ingest_run_files (
  run_id STRING NOT NULL,
  source STRING NOT NULL,
  file_name STRING NOT NULL,
  action STRING NOT NULL,
  sha256_before STRING,
  sha256_after STRING,
  pointer_flipped BOOLEAN NOT NULL,
  http_status INT,
  bytes BIGINT,
  elapsed_ms BIGINT NOT NULL,
  error STRING,
  recorded_at TIMESTAMP NOT NULL,
  CONSTRAINT ingest_run_files_pk PRIMARY KEY (run_id, source, file_name)
) COMMENT 'One row per run and file: what the run did with it.';

CREATE TABLE IF NOT EXISTS quest_ops.file_versions (
  source STRING NOT NULL,
  file_name STRING NOT NULL,
  sha256 STRING NOT NULL,
  file_role STRING,
  landing_path STRING NOT NULL,
  size_bytes BIGINT NOT NULL,
  line_count BIGINT NOT NULL,
  http_last_modified TIMESTAMP,
  request_url STRING NOT NULL,
  content_type STRING NOT NULL,
  header_ok BOOLEAN NOT NULL,
  header_observed STRING NOT NULL,
  header_diff STRING,
  first_seen_run_id STRING NOT NULL,
  first_seen_at TIMESTAMP NOT NULL,
  CONSTRAINT file_versions_pk PRIMARY KEY (source, file_name, sha256)
) COMMENT 'One row per distinct landed blob; insert-only.';

CREATE TABLE IF NOT EXISTS quest_ops.file_state (
  source STRING NOT NULL,
  file_name STRING NOT NULL,
  current_sha256 STRING NOT NULL,
  status STRING NOT NULL,
  consecutive_missing INT NOT NULL,
  listing_size BIGINT,
  listing_ts_raw STRING,
  http_last_modified TIMESTAMP,
  last_checked_run_id STRING NOT NULL,
  last_changed_run_id STRING NOT NULL,
  missing_since_run_id STRING,
  updated_at TIMESTAMP NOT NULL,
  CONSTRAINT file_state_pk PRIMARY KEY (source, file_name)
) COMMENT 'Current pointer per logical file; silver serves these versions.';

CREATE TABLE IF NOT EXISTS quest_ops.table_versions (
  run_id STRING NOT NULL,
  table_name STRING NOT NULL,
  layer STRING NOT NULL,
  delta_version BIGINT,
  row_count BIGINT NOT NULL,
  content_checksum DECIMAL(38,0) NOT NULL,
  changed_since_previous BOOLEAN NOT NULL,
  captured_at TIMESTAMP NOT NULL,
  CONSTRAINT table_versions_pk PRIMARY KEY (run_id, table_name)
) COMMENT 'One row per validate run and data table: Delta version, rows, checksum.';

CREATE TABLE IF NOT EXISTS quest_ops.dq_checks (
  run_id STRING NOT NULL,
  check_code STRING NOT NULL,
  table_name STRING NOT NULL,
  layer STRING NOT NULL,
  severity STRING NOT NULL,
  status STRING NOT NULL,
  observed_value STRING NOT NULL,
  threshold STRING,
  failing_rows BIGINT,
  details STRING,
  checked_at TIMESTAMP NOT NULL,
  CONSTRAINT dq_checks_pk PRIMARY KEY (run_id, check_code, table_name)
) COMMENT 'One row per validate run, check and table.';

ALTER TABLE quest_ops.ingest_runs DROP CONSTRAINT IF EXISTS ingest_runs_status;

ALTER TABLE quest_ops.ingest_runs ADD CONSTRAINT ingest_runs_status CHECK (status IN ('running', 'succeeded', 'failed'));

ALTER TABLE quest_ops.ingest_run_files DROP CONSTRAINT IF EXISTS ingest_run_files_action;

ALTER TABLE quest_ops.ingest_run_files ADD CONSTRAINT ingest_run_files_action CHECK (action IN ('SKIPPED_FINGERPRINT', 'NOT_MODIFIED', 'SAME_HASH', 'NEW_VERSION', 'REVERTED_TO_KNOWN_VERSION', 'SELF_HEALED', 'MISSING', 'DELETED', 'FAILED'));

ALTER TABLE quest_ops.file_state DROP CONSTRAINT IF EXISTS file_state_status;

ALTER TABLE quest_ops.file_state ADD CONSTRAINT file_state_status CHECK (status IN ('active', 'missing', 'deleted'));

ALTER TABLE quest_ops.dq_checks DROP CONSTRAINT IF EXISTS dq_checks_severity;

ALTER TABLE quest_ops.dq_checks ADD CONSTRAINT dq_checks_severity CHECK (severity IN ('warn', 'fail'));

ALTER TABLE quest_ops.dq_checks DROP CONSTRAINT IF EXISTS dq_checks_status;

ALTER TABLE quest_ops.dq_checks ADD CONSTRAINT dq_checks_status CHECK (status IN ('pass', 'warn', 'fail', 'error'));
