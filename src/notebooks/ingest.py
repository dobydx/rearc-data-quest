# Databricks notebook source
"""Ingest job task: read the widgets and run ``quest.ingest.main``."""

from databricks.sdk.runtime import dbutils

# quest comes from the wheel the job environment installs (bundle artifact quest_wheel).
from quest.ingest.main import INGEST_WIDGETS, main, notebook_argv

# COMMAND ----------

# The job fills every widget. base_parameters supply catalog, landing_root, run_id, job_id and
# trigger; the job parameters force_pipeline and code_version are pushed down as widgets too.
# Run by hand, fill catalog and landing_root. An empty code_version falls back to the installed
# package version, which never equals the job's git-<sha>, so a manual run always fires the gate.
for name in INGEST_WIDGETS:
    dbutils.widgets.text(name, "")
exit_code = main(notebook_argv({name: dbutils.widgets.get(name) for name in INGEST_WIDGETS}))
if exit_code != 0:
    raise SystemExit(exit_code)
