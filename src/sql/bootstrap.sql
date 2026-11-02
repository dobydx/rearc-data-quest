-- One-time workspace bootstrap: the five quest schemas and the landing Volume.
-- Idempotent (IF NOT EXISTS), so a rerun changes nothing. Tables are created by the code:
-- quest_ops by the ingest task, bronze/silver/gold by the pipeline.

CREATE SCHEMA IF NOT EXISTS workspace.quest_raw
COMMENT 'Raw landing zone: the landing Volume with every downloaded file version, content-addressed by sha256.';

CREATE SCHEMA IF NOT EXISTS workspace.quest_ops
COMMENT 'Operational metadata: file versions and state, ingest run audit, table versions, DQ checks, pipeline event log.';

CREATE SCHEMA IF NOT EXISTS workspace.quest_bronze
COMMENT 'Bronze streaming tables: every landed file version, one row per line, all STRING, verbatim.';

CREATE SCHEMA IF NOT EXISTS workspace.quest_silver
COMMENT 'Silver tables (current versions, typed, canonical labels) and their _quarantine twins.';

CREATE SCHEMA IF NOT EXISTS workspace.quest_gold
COMMENT 'Gold answers to the quest questions plus the denormalized productivity fact.';

CREATE VOLUME IF NOT EXISTS workspace.quest_raw.landing
COMMENT 'Landed source files: <source>/<file_name>/sha256=<hash>/<file_name>.';
