"""Ties the bundle's job definition to the parsers its tasks call, so a renamed flag fails here."""

import ast
import re
import tomllib
from pathlib import Path
from typing import Any

import pytest
import yaml

from quest.config import parse_ingest_args, parse_validate_args
from quest.ingest.main import INGEST_WIDGETS, notebook_argv

ROOT = Path(__file__).parents[2]
JOB_YAML = ROOT / "resources" / "bls_datausa_weekly_ingest.job.yml"

# One sample per placeholder the job uses. A new placeholder without a sample fails the test.
SAMPLES = {
    "${var.catalog}": "workspace",
    "${var.landing_root}": "/Volumes/workspace/quest_raw/landing",
    "${var.code_version}": "git-0123abc",
    "${var.notification_email}": "someone@example.com",
    "${resources.pipelines.bls_datausa_weekly_pipeline.id}": "pipeline-id",
    "{{job.run_id}}": "111",
    "{{job.id}}": "222",
    "{{job.trigger.type}}": "PERIODIC",
    "{{tasks.ingest.values.run_pipeline}}": "true",
    "{{tasks.ingest.values.pointers_flipped}}": "3",
    "{{tasks.ingest.values.code_changed}}": "true",
}
_PLACEHOLDER = re.compile(r"\$\{[^}]+\}|\{\{[^}]+\}\}")


@pytest.fixture(scope="module")
def job() -> dict[str, Any]:
    text = _PLACEHOLDER.sub(lambda m: SAMPLES[m.group(0)], JOB_YAML.read_text())
    loaded: dict[str, Any] = yaml.safe_load(text)["resources"]["jobs"]["bls_datausa_weekly_ingest"]
    return loaded


def _task(job: dict[str, Any], key: str) -> dict[str, Any]:
    (task,) = [t for t in job["tasks"] if t["task_key"] == key]
    return task


def _ingest_widget_values(job: dict[str, Any]) -> dict[str, str]:
    # Job parameters are pushed down to notebook tasks as widgets, next to base_parameters.
    pushed_down = {p["name"]: p["default"] for p in job["parameters"]}
    return pushed_down | _task(job, "ingest")["notebook_task"]["base_parameters"]


def test_job_fills_exactly_the_ingest_notebook_widgets(job: dict[str, Any]) -> None:
    # Equal, not only a subset: a widget the job never fills would run with an empty value.
    assert set(_ingest_widget_values(job)) == set(INGEST_WIDGETS)


def test_ingest_notebook_builds_its_widgets_and_argv_from_the_shared_names(
    job: dict[str, Any],
) -> None:
    # Bundle paths are relative to the YAML file's folder.
    notebook = JOB_YAML.parent / _task(job, "ingest")["notebook_task"]["notebook_path"]
    tree = ast.parse(notebook.read_text())

    loops = [n for n in ast.walk(tree) if isinstance(n, ast.For)]
    assert any(
        ast.unparse(loop.iter) == "INGEST_WIDGETS" and "dbutils.widgets.text(" in ast.unparse(loop)
        for loop in loops
    )
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)]
    argv_calls = [c for c in calls if ast.unparse(c.func) == "notebook_argv"]
    assert [ast.unparse(c.args[0]) for c in argv_calls] == [
        "{name: dbutils.widgets.get(name) for name in INGEST_WIDGETS}"
    ]


def test_ingest_widget_values_parse(job: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BLS_CONTACT_EMAIL", "someone@example.com")

    cfg = parse_ingest_args(notebook_argv(_ingest_widget_values(job)))

    assert (cfg.catalog, str(cfg.landing_root)) == (
        "workspace",
        "/Volumes/workspace/quest_raw/landing",
    )
    assert (cfg.run_id, cfg.job_run_id, cfg.job_id) == ("111", "111", "222")
    assert cfg.trigger == "PERIODIC"
    assert (cfg.code_version, cfg.force_pipeline) == ("git-0123abc", False)


def test_validate_wheel_parameters_parse(job: dict[str, Any]) -> None:
    cfg = parse_validate_args(_task(job, "validate")["python_wheel_task"]["parameters"])

    assert (cfg.catalog, cfg.run_id, cfg.ingest_pointers_flipped) == ("workspace", "111", 3)
    assert cfg.ingest_code_changed is True


def test_validate_entry_point_is_a_console_script(job: dict[str, Any]) -> None:
    scripts = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["scripts"]

    assert _task(job, "validate")["python_wheel_task"]["entry_point"] in scripts
