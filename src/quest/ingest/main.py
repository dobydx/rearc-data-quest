"""Ingest entry point: sync every source, flip pointers all-or-nothing, audit, gate the pipeline."""

import json
import sys
import uuid
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import datetime

from quest.config import IngestConfig, parse_ingest_args, utc_now
from quest.contracts.loader import contract_for_file, silver_contracts
from quest.ingest import bls, datausa
from quest.ingest.http import HttpClient, user_agent
from quest.ingest.models import (
    SERVED_STATUSES,
    FileState,
    IngestRun,
    RunFile,
    RunFileAction,
    RunStatus,
)
from quest.ingest.ops_store import OpsStore
from quest.ingest.planner import code_changed, should_run_pipeline, state_changed
from quest.log import configure_logging, get_logger
from quest.runtime import dbutils

# Job task values the condition task and validate read: {{tasks.ingest.values.<key>}}.
TASK_VALUE_KEYS = ("run_pipeline", "pointers_flipped", "code_changed")
# Ingest notebook widget -> parse_ingest_args flag. The job fills every widget, from
# base_parameters and from job parameters (which Databricks pushes down to notebook tasks).
INGEST_WIDGETS: Mapping[str, str] = {
    "catalog": "--catalog",
    "landing_root": "--landing-root",
    "force_pipeline": "--force-pipeline",
    "code_version": "--code-version",
    "run_id": "--job-run-id",
    "job_id": "--job-id",
    "trigger": "--trigger",
}


class IngestFailedError(Exception):
    """Raised when at least one file failed, so no pointer was flipped."""


def notebook_argv(
    widgets: Mapping[str, str], new_run_id: Callable[[], str] = lambda: uuid.uuid4().hex
) -> list[str]:
    """Turn the ingest notebook's widget values into ``parse_ingest_args`` arguments.

    Empty widgets are left out, so ``IngestConfig`` keeps its defaults. A job run uses
    ``{{job.run_id}}`` as the run id too; a manual run gets a fresh one.
    """
    argv = ["--run-id", widgets.get("run_id") or new_run_id()]
    for name, flag in INGEST_WIDGETS.items():
        if value := widgets.get(name):
            argv += [flag, value]
    return argv


def run_ingest(
    cfg: IngestConfig,
    client: HttpClient,
    store: OpsStore,
    now: Callable[[], datetime] = utc_now,
) -> IngestRun:
    """Run both syncs and commit every pointer in one write, only if every file succeeded.

    Raises:
        IngestFailedError: If any file failed; versions stay recorded, pointers do not move.
    """
    store.ensure_tables()
    run = IngestRun(
        run_id=cfg.run_id,
        job_id=cfg.job_id,
        job_run_id=cfg.job_run_id,
        started_at=now(),
        status=RunStatus.RUNNING,
        code_version=cfg.code_version,
        trigger=cfg.trigger,
        params_json=_params_json(cfg),
    )
    store.start_run(run)
    try:
        before = _states(store)
        results = [
            bls.sync_bls(client, store, cfg, now),
            datausa.sync_datausa(client, store, cfg, now),
        ]
        failed = any(result.failed for result in results)
        run_files = [rf for result in results for rf in result.run_files]
        # Audit first, so a crash in the MERGE below still leaves what each file did.
        store.record_run_files(run_files)
        if not failed:
            changed = [
                s
                for result in results
                for s in result.next_states
                if state_changed(before.get((s.source, s.file_name)), s)
            ]
            store.commit_states(changed)
            after = before | {(s.source, s.file_name): s for s in changed}
            run_files = [
                replace(rf, pointer_flipped=_served(before, rf) != _served(after, rf))
                for rf in run_files
            ]
            store.record_run_files(run_files)
    except Exception as exc:
        # Record the failed run, then let the original error fail the task.
        store.finish_run(_failed(run, now(), type(exc).__name__, str(exc)))
        raise
    ended_at = now()
    finished = _with_counts(
        replace(run, status=RunStatus.SUCCEEDED, ended_at=ended_at),
        run_files,
        versions_landed=sum(len(result.versions) for result in results),
        files_listed=sum(result.files_listed for result in results),
    )
    failures = [f"{rf.source}/{rf.file_name} ({rf.error})" for rf in run_files if rf.error]
    if failed:
        message = "failed files: " + ", ".join(failures)
        finished = _failed(finished, ended_at, "IngestFailedError", message)
    store.finish_run(finished)
    get_logger(__name__).info("ingest_finished", status=str(finished.status), **_counts(finished))
    if failed:
        raise IngestFailedError(finished.error_message)
    return finished


def silver_pointers(store: OpsStore) -> set[tuple[str, str]]:
    """Return ``(file_name, sha256)`` of every served pointer whose file feeds a silver table."""
    pointers = set()
    for source in (bls.SOURCE, datausa.SOURCE):
        for name, state in store.file_states(source).items():
            contract = contract_for_file(name)
            if state.status in SERVED_STATUSES and contract in silver_contracts():
                pointers.add((name, state.current_sha256))
    return pointers


@dataclass(frozen=True)
class GateDecision:
    """What the pipeline gate decided, published as task values."""

    run_pipeline: bool
    # The deployed code differs from the last update's, so validate expects tables to move.
    code_changed: bool


def pipeline_gate(cfg: IngestConfig, store: OpsStore) -> GateDecision:
    """Decide whether the pipeline runs: pending pointer, failed update, new code, or forced."""
    last_update = store.last_pipeline_update()
    run_pipeline = should_run_pipeline(
        pointers=silver_pointers(store),
        silver_lineage=store.silver_lineage(),
        last_update=last_update,
        code_version=cfg.code_version,
        force=cfg.force_pipeline,
    )
    return GateDecision(run_pipeline, code_changed(last_update, cfg.code_version))


def publish_task_values(values: Mapping[str, bool | int]) -> None:
    """Set job task values for the condition task; off Databricks, only log them."""
    get_logger(__name__).info("task_values", **values)
    handle = dbutils()
    if handle is None:
        return
    for key, value in values.items():
        handle.jobs.taskValues.set(key=key, value=value)


def main(argv: Sequence[str] | None = None) -> int:
    """Run ingest as a job task and publish the ``TASK_VALUE_KEYS`` task values."""
    cfg = parse_ingest_args(sys.argv[1:] if argv is None else argv)
    configure_logging(cfg.run_id)
    # Imported here so unit tests and local runs never load pyspark.
    from quest.ingest.delta_ops_store import DeltaOpsStore
    from quest.spark import get_spark

    store = DeltaOpsStore(get_spark(), cfg.catalog)
    with HttpClient(
        user_agent=user_agent(cfg.contact_email),
        connect_timeout_s=cfg.connect_timeout_s,
        read_timeout_s=cfg.read_timeout_s,
        max_attempts=cfg.max_attempts,
        min_interval_s=cfg.min_request_interval_s,
    ) as client:
        run = run_ingest(cfg, client, store)
    gate = pipeline_gate(cfg, store)
    run_pipeline, pointers_flipped, changed_code = TASK_VALUE_KEYS
    publish_task_values(
        {
            run_pipeline: gate.run_pipeline,
            pointers_flipped: run.pointers_flipped,
            changed_code: gate.code_changed,
        }
    )
    return 0


def _served(states: Mapping[tuple[str, str], FileState], rf: RunFile) -> str | None:
    """The sha silver serves for a file, or None when it serves nothing (absent or deleted)."""
    state = states.get((rf.source, rf.file_name))
    return state.current_sha256 if state and state.status in SERVED_STATUSES else None


def _params_json(cfg: IngestConfig) -> str:
    # The contact email is personal data, so it stays out of the audit table.
    params = {key: value for key, value in asdict(cfg).items() if key != "contact_email"}
    return json.dumps(params, default=str, sort_keys=True)


def _failed(run: IngestRun, ended_at: datetime, error_class: str, message: str) -> IngestRun:
    return replace(
        run,
        status=RunStatus.FAILED,
        ended_at=ended_at,
        error_class=error_class,
        error_message=message,
    )


def _with_counts(
    run: IngestRun, run_files: Sequence[RunFile], versions_landed: int, files_listed: int
) -> IngestRun:
    actions = Counter(rf.action for rf in run_files)
    return replace(
        run,
        files_listed=files_listed,
        files_skipped_fingerprint=actions[RunFileAction.SKIPPED_FINGERPRINT],
        files_not_modified=actions[RunFileAction.NOT_MODIFIED],
        files_same_hash=actions[RunFileAction.SAME_HASH],
        versions_landed=versions_landed,
        pointers_flipped=sum(1 for rf in run_files if rf.pointer_flipped),
        files_missing=actions[RunFileAction.MISSING],
        files_deleted=actions[RunFileAction.DELETED],
        files_self_healed=actions[RunFileAction.SELF_HEALED],
        files_failed=actions[RunFileAction.FAILED],
    )


def _states(store: OpsStore) -> dict[tuple[str, str], FileState]:
    return {
        (s.source, s.file_name): s
        for source in (bls.SOURCE, datausa.SOURCE)
        for s in store.file_states(source).values()
    }


def _counts(run: IngestRun) -> dict[str, int]:
    return {
        key: value
        for key, value in asdict(run).items()
        if key.startswith(("files_", "versions_", "pointers_"))
    }
