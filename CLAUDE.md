# CLAUDE.md

Project conventions for anyone changing this repo. README.md says what the project does; PROCESS.md says why it is built this way.

## Layout

```
src/quest/                 Python package, installed as a wheel on Databricks
  config.py                frozen dataclass configs parsed from job parameters (argparse)
  contracts/               YAML file contracts + rules.yml, loaded into frozen dataclasses
  ingest/                  HTTP client, listing parser, planner, landing, ops store, BLS and DataUSA syncs
  transforms/              pure DataFrame functions for bronze, silver and dq_reasons
  gold/                    PySpark twins of the gold SQL, and the gold.sql body loader
  validate/                post-pipeline checks written to quest_ops.dq_checks
src/pipeline/transformations/   bronze.py, silver.py (thin) and gold.sql (primary gold)
src/notebooks/ingest.py    ingest job task (a notebook, because only notebooks set task values)
src/sql/                   bootstrap.sql, dashboard dataset queries, the DQ alert query
scripts/build_bi.py        builds the dashboard and Genie JSON from src/sql/dashboard and src/genie
src/dashboards/            AI/BI dashboard definition
src/genie/                 Genie Agent instructions, sample questions, trusted SQL
resources/                 bundle resources: job, pipeline, dashboard, alert, Genie space
tests/unit                 pure Python tests
tests/spark                local PySpark tests (marker spark); connect_safe ones also run on serverless
tests/fixtures             small real slices of the source files; byte-exact, never reformat
```

## Commands

```
make setup          venv data-quest-uv-venv with all groups, plus git hooks
make lint           ruff check and ruff format --check
make typecheck      mypy --strict on src/
make test           unit tests, then local Spark tests (needs JDK 17)
make cov            all local tests with a coverage report
make bi             regenerate the dashboard and Genie JSON (scripts/build_bi.py)
make setup-connect  second venv for databricks-connect (it conflicts with plain pyspark)
make test-connect   connect_safe Spark tests on serverless
make deploy         databricks bundle deploy -t dev (refuses a dirty tree)
make run            run the ingest job on dev and wait
```

Keep `make lint typecheck test` green on every commit.

## Hard rules

- Idempotent everywhere: a rerun with no upstream or code change downloads nothing and changes no table. The validate check NO_CHANGE_ON_RERUN enforces it.
- Never drop a bad row silently. It goes to the `<table>_quarantine` twin with a reason code, or the run fails.
- Every gold answer exists in Spark SQL (`gold.sql`) and PySpark (`quest.gold`), and a test asserts they return identical rows. The dashboard and Genie read the gold tables and never recompute an answer.
- `src/dashboards/*.lvdash.json` and `src/genie/*.geniespace.json` are generated: edit `src/sql/dashboard/*.sql` or `src/genie/*`, then `make bi`. Tests fail while the JSON is stale.
- Pure transform logic lives in `src/quest` and is unit-tested locally. Pipeline and notebook files stay thin.
- Every HTTP call has a timeout and retries with backoff; 403 and 404 are never retried. BLS requests carry a contact User-Agent and are at least 1 s apart.
- No hardcoded file names, hosts or secrets. Configuration comes from job parameters, pipeline configuration or bundle variables. The BLS contact email is the secret `quest/bls_contact_email`.
- `code_version` is `git-<sha>` from the bundle; deploying uncommitted code would mislabel it, so commit first.
- Unity Catalog: catalog from the `catalog` variable, schemas `quest_raw`, `quest_ops`, `quest_bronze`, `quest_silver`, `quest_gold`. Never `default`.
- Verify Databricks syntax against docs.databricks.com before using it; product names change often.

## Code style

- Python 3.12, type hints everywhere, mypy strict on `src/`, ruff rules in pyproject.toml, line length 100.
- Google docstrings on public functions, one line unless Args/Returns/Raises add information. Comments explain why, not what.
- structlog JSON logging with the run id bound. No bare `except`.
- Boring, explicit code over clever code. DRY and KISS.
- Tests first: a change starts from a failing test.

## Commits

Conventional Commits (`feat:`, `fix:`, `test:`, `ci:`, `docs:`, `chore:`).
