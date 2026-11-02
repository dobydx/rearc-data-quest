import json
from types import SimpleNamespace

import pytest

from quest.ingest import main as ingest_main
from quest.ingest.main import (
    INGEST_WIDGETS,
    notebook_argv,
    pipeline_gate,
    publish_task_values,
    silver_pointers,
)
from quest.ingest.models import FileStatus
from quest.ingest.ops_store import InMemoryOpsStore
from quest.ingest.planner import PipelineUpdateInfo
from quest.log import configure_logging
from tests.unit.factories import make_config, state


def test_silver_pointers_only_counts_files_that_feed_silver() -> None:
    store = InMemoryOpsStore()
    store.commit_states(
        [
            state("pr.data.1.AllData", current_sha256="1" * 64),
            state("pr.series", current_sha256="2" * 64, status=FileStatus.MISSING),
            state("pr.class", current_sha256="3" * 64, status=FileStatus.DELETED),
            state("pr.data.0.Current", current_sha256="4" * 64),
            state("pr.txt", current_sha256="5" * 64),
            state("pr.data.2.New", current_sha256="6" * 64),
        ]
    )

    assert silver_pointers(store) == {("pr.data.1.AllData", "1" * 64), ("pr.series", "2" * 64)}


def test_publish_task_values_logs_off_databricks(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("r1")

    publish_task_values({"run_pipeline": True, "pointers_flipped": 2})

    event = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert event["event"] == "task_values"
    assert event["run_pipeline"] is True
    assert event["pointers_flipped"] == 2


def test_publish_task_values_sets_each_value(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, object]] = []
    task_values = SimpleNamespace(set=lambda key, value: calls.append((key, value)))
    fake = SimpleNamespace(jobs=SimpleNamespace(taskValues=task_values))
    monkeypatch.setattr(ingest_main, "dbutils", lambda: fake)

    publish_task_values({"run_pipeline": False, "pointers_flipped": 0})

    assert calls == [("run_pipeline", False), ("pointers_flipped", 0)]


@pytest.mark.parametrize(("deployed", "expected"), [("2.0", False), ("2.1", True)])
def test_gate_compares_the_configured_code_version(deployed: str, expected: bool) -> None:
    store = InMemoryOpsStore()
    store.commit_states([state("pr.series", current_sha256="2" * 64)])
    store.silver_lineage_value = {("pr.series", "2" * 64)}
    store.last_update_value = PipelineUpdateInfo(state="COMPLETED", code_version="2.0")

    gate = pipeline_gate(make_config(code_version=deployed), store)

    # Same comparison twice: the gate reruns the pipeline, and validate expects tables to move.
    assert (gate.run_pipeline, gate.code_changed) == (expected, expected)


def test_task_values_name_what_the_gate_and_validate_read() -> None:
    assert ingest_main.TASK_VALUE_KEYS == ("run_pipeline", "pointers_flipped", "code_changed")


def test_notebook_argv_reuses_the_job_run_id_and_skips_empty_widgets() -> None:
    widgets = {name: "" for name in INGEST_WIDGETS} | {
        "catalog": "workspace",
        "landing_root": "/Volumes/x",
        "run_id": "111",
    }

    assert notebook_argv(widgets) == [
        "--run-id", "111",
        "--catalog", "workspace",
        "--landing-root", "/Volumes/x",
        "--job-run-id", "111",
    ]  # fmt: skip


def test_notebook_argv_gives_a_manual_run_a_fresh_run_id() -> None:
    argv = notebook_argv({"catalog": "workspace", "landing_root": "/Volumes/x"}, lambda: "fresh")

    assert argv == ["--run-id", "fresh", "--catalog", "workspace", "--landing-root", "/Volumes/x"]
