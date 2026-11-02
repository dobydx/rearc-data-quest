export UV_PROJECT_ENVIRONMENT := data-quest-uv-venv
# ?= so CI (setup-java) and other machines can bring their own JDK 17.
export JAVA_HOME ?= /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home

.DEFAULT_GOAL := help
.PHONY: help install setup setup-connect lint fmt typecheck test test-unit test-spark test-connect cov bi bundle-validate deploy run

help:  ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "%-16s %s\n", $$1, $$2}'

install:  ## Create or update the venv from uv.lock, all dependency groups (CI uses this)
	uv sync --all-groups --locked

setup: install  ## install, then the git hooks
	uv run pre-commit install

# databricks-connect ships its own pyspark and conflicts with the plain one, hence a second venv.
# 19.x matches serverless environment version 6, which the job uses.
setup-connect:  ## Create data-quest-connect-venv with databricks-connect, pytest and quest
	uv venv data-quest-connect-venv --python 3.12 --allow-existing
	VIRTUAL_ENV=data-quest-connect-venv uv pip install "databricks-connect~=19.3.0" pytest -e .

lint:  ## Ruff check and format check
	uv run ruff check . && uv run ruff format --check .

fmt:  ## Apply ruff fixes and formatting
	uv run ruff check --fix . && uv run ruff format .

typecheck:  ## mypy --strict on src/
	uv run mypy src

test: test-unit test-spark  ## All tests

test-unit:  ## Unit tests (no Spark)
	uv run pytest tests/unit

test-spark:  ## Local PySpark tests (needs JDK 17)
	uv run pytest tests/spark -m spark

test-connect:  ## connect_safe Spark tests on serverless via databricks-connect (default profile)
	QUEST_SPARK=connect data-quest-connect-venv/bin/pytest tests/spark -m "spark and connect_safe"

cov:  ## All local tests (unit and Spark) with a coverage report, no threshold
	uv run pytest --cov --cov-report=term-missing --cov-report=html

bi:  ## Regenerate the dashboard and Genie JSON from src/sql/dashboard and src/genie
	uv run python scripts/build_bi.py

bundle-validate:  ## Validate the bundle for the dev target
	databricks bundle validate -t dev

# code_version defaults to git-<HEAD sha> (bundle variable). Deploying uncommitted changes would
# label new code with the old sha, and the pipeline gate would skip the pipeline, so refuse.
deploy:  ## Build the wheel and deploy the bundle to dev (clean tree only)
	@test -z "$$(git status --porcelain)" || { echo "commit your changes first: code_version is the HEAD sha"; exit 1; }
	databricks bundle deploy -t dev

run:  ## Run the ingest job on dev and wait for it
	databricks bundle run -t dev bls_datausa_weekly_ingest
