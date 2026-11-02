from dataclasses import replace

from quest.ingest.models import (
    FileStatus,
    FileVersion,
    IngestRun,
    RunFile,
    RunFileAction,
    RunStatus,
)
from quest.ingest.ops_store import InMemoryOpsStore, OpsStore
from tests.unit.factories import NOW, state


def _version(sha: str, file_name: str = "pr.series") -> FileVersion:
    return FileVersion(
        source="bls_pr",
        file_name=file_name,
        sha256=sha,
        file_role="series",
        landing_path=f"/l/bls_pr/{file_name}/sha256={sha}/{file_name}",
        size_bytes=10,
        line_count=1,
        http_last_modified=NOW,
        request_url=f"https://x/{file_name}",
        content_type="application/octet-stream",
        header_ok=True,
        header_observed="series_id",
        header_diff=None,
        first_seen_run_id="r1",
        first_seen_at=NOW,
    )


def test_in_memory_store_satisfies_protocol() -> None:
    store: OpsStore = InMemoryOpsStore()

    store.ensure_tables()


def test_insert_version_idempotent() -> None:
    store = InMemoryOpsStore()

    store.insert_version(_version("a" * 64))
    store.insert_version(replace(_version("a" * 64), first_seen_run_id="r2"))
    store.insert_version(_version("b" * 64))

    assert store.known_shas("bls_pr", "pr.series") == {"a" * 64, "b" * 64}
    assert store.versions[("bls_pr", "pr.series", "a" * 64)].first_seen_run_id == "r1"
    assert store.known_shas("bls_pr", "pr.class") == set()


def test_commit_states_upserts_all() -> None:
    store = InMemoryOpsStore()
    store.commit_states([state("pr.series"), state("pr.class")])

    store.commit_states([state("pr.series", status=FileStatus.MISSING), state("pr.period")])

    states = store.file_states("bls_pr")
    assert set(states) == {"pr.series", "pr.class", "pr.period"}
    assert states["pr.series"].status is FileStatus.MISSING
    assert store.file_states("datausa") == {}


def test_runs_and_run_files_are_recorded() -> None:
    store = InMemoryOpsStore()
    run = IngestRun(
        run_id="r1",
        job_id=None,
        job_run_id=None,
        started_at=NOW,
        status=RunStatus.RUNNING,
        code_version="1.0",
        trigger="local",
        params_json="{}",
    )
    run_file = RunFile(
        run_id="r1",
        source="bls_pr",
        file_name="pr.series",
        action=RunFileAction.NEW_VERSION,
        sha256_before=None,
        sha256_after="a" * 64,
        pointer_flipped=True,
        http_status=200,
        bytes=10,
        elapsed_ms=5,
        error=None,
        recorded_at=NOW,
    )

    store.start_run(run)
    store.record_run_files([run_file])
    store.finish_run(replace(run, status=RunStatus.SUCCEEDED, ended_at=NOW))

    assert store.runs["r1"].status is RunStatus.SUCCEEDED
    assert store.run_files == {("r1", "bls_pr", "pr.series"): run_file}


def test_lineage_and_last_update_default_to_none() -> None:
    store = InMemoryOpsStore()

    assert store.silver_lineage() is None
    assert store.last_pipeline_update() is None


def test_served_shas_come_from_flipped_pointers() -> None:
    store = InMemoryOpsStore()
    base = RunFile(
        run_id="r1",
        source="bls_pr",
        file_name="pr.series",
        action=RunFileAction.NEW_VERSION,
        sha256_before=None,
        sha256_after="a" * 64,
        pointer_flipped=True,
        http_status=200,
        bytes=10,
        elapsed_ms=5,
        error=None,
        recorded_at=NOW,
    )
    store.record_run_files(
        [base, replace(base, run_id="r2", sha256_after="b" * 64, pointer_flipped=False)]
    )

    assert store.served_shas("bls_pr", "pr.series") == {"a" * 64}
    assert store.served_shas("bls_pr", "pr.class") == set()
