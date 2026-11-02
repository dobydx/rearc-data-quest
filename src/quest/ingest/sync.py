"""Steps shared by the BLS and DataUSA syncs: accept a download, or record why not."""

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path

from quest.contracts.header import HeaderCheck
from quest.contracts.loader import contract_for_file
from quest.ingest.http import ForbiddenError, HttpError, HttpResponse, parse_http_date
from quest.ingest.landing import land, sha256_hex
from quest.ingest.models import (
    FileState,
    FileStatus,
    FileVersion,
    RemoteFile,
    RunFile,
    RunFileAction,
)
from quest.ingest.ops_store import OpsStore
from quest.ingest.planner import classify_download
from quest.log import get_logger


class FileRejectedError(Exception):
    """A response arrived but cannot be accepted as a file version."""


class LengthMismatchError(FileRejectedError):
    """The body length differs from the listing size or Content-Length (a truncated GET)."""


# Errors that fail one file; anything else (including ForbiddenError) fails the whole run.
FILE_ERRORS = (HttpError, OSError, FileRejectedError)


@dataclass(frozen=True)
class SyncContext:
    """What every per-file step needs from the run."""

    store: OpsStore
    landing_root: Path
    run_id: str
    now: Callable[[], datetime]


@dataclass(frozen=True)
class FileOutcome:
    """One file's audit row, plus the version it landed and the state it proposes, if any."""

    run_file: RunFile
    version: FileVersion | None = None
    next_state: FileState | None = None


@dataclass(frozen=True)
class SyncResult:
    """Everything one source sync produced; ``failed`` blocks the pointer flip."""

    versions: list[FileVersion]
    run_files: list[RunFile]
    next_states: list[FileState]
    failed: bool
    files_listed: int = 0

    @classmethod
    def from_outcomes(cls, outcomes: Sequence[FileOutcome], files_listed: int = 0) -> "SyncResult":
        """Collect per-file outcomes into one result; ``versions`` holds only new rows."""
        return cls(
            versions=[o.version for o in outcomes if o.version is not None],
            run_files=[o.run_file for o in outcomes],
            next_states=[o.next_state for o in outcomes if o.next_state is not None],
            failed=any(o.run_file.action is RunFileAction.FAILED for o in outcomes),
            files_listed=files_listed,
        )


def run_file(
    ctx: SyncContext,
    source: str,
    file_name: str,
    action: RunFileAction,
    state: FileState | None,
    *,
    sha256_after: str | None = None,
    http_status: int | None = None,
    size: int | None = None,
    error: str | None = None,
) -> RunFile:
    """Build an audit row; ``pointer_flipped`` and ``elapsed_ms`` are filled in later."""
    return RunFile(
        run_id=ctx.run_id,
        source=source,
        file_name=file_name,
        action=action,
        sha256_before=state.current_sha256 if state else None,
        sha256_after=sha256_after,
        pointer_flipped=False,
        http_status=http_status,
        bytes=size,
        elapsed_ms=0,
        error=error,
        recorded_at=ctx.now(),
    )


def guarded(
    ctx: SyncContext,
    source: str,
    file_name: str,
    state: FileState | None,
    step: Callable[[], FileOutcome],
) -> FileOutcome:
    """Run one file's step, timing it; a file-level error becomes a FAILED row, not a crash."""
    log = get_logger(__name__)
    started = time.monotonic()
    try:
        outcome = step()
    except ForbiddenError:
        raise
    except FILE_ERRORS as exc:
        log.error("file_failed", source=source, file_name=file_name, error=repr(exc))
        error = f"{type(exc).__name__}: {exc}"
        outcome = FileOutcome(
            run_file(ctx, source, file_name, RunFileAction.FAILED, state, error=error)
        )
    elapsed_ms = round((time.monotonic() - started) * 1000)
    log.info(
        "file_synced",
        source=source,
        file_name=file_name,
        action=str(outcome.run_file.action),
        elapsed_ms=elapsed_ms,
    )
    return replace(outcome, run_file=replace(outcome.run_file, elapsed_ms=elapsed_ms))


def check_length(response: HttpResponse, listed_size: int | None) -> None:
    """Reject a body whose length differs from the listing or from Content-Length.

    Content-Length is skipped under Content-Encoding, where it counts compressed bytes.

    Raises:
        LengthMismatchError: On either mismatch.
    """
    actual = len(response.body)
    if listed_size is not None and actual != listed_size:
        raise LengthMismatchError(f"got {actual} bytes, listing says {listed_size}")
    declared = response.headers.get("content-length")
    compressed = "content-encoding" in response.headers
    # Compared as text so a garbled header is a mismatch, not a ValueError.
    if declared is not None and not compressed and declared.strip() != str(actual):
        raise LengthMismatchError(f"got {actual} bytes, Content-Length says {declared}")


def accept_body(
    ctx: SyncContext,
    *,
    source: str,
    file_name: str,
    url: str,
    response: HttpResponse,
    state: FileState | None,
    header: HeaderCheck,
    line_count: int,
    remote: RemoteFile | None,
) -> FileOutcome:
    """Land a 200 body, record a new version if it is one, and propose the next pointer.

    A bad header still lands and records its version, but proposes no state.
    ``FileOutcome.version`` is set only when this call inserted the version row.
    """
    sha256 = sha256_hex(response.body)
    action = classify_download(state, ctx.store.served_shas(source, file_name), sha256)
    is_new_blob = sha256 not in ctx.store.known_shas(source, file_name)
    landed = land(ctx.landing_root, source, file_name, response.body)
    if action is RunFileAction.SAME_HASH and landed.written:
        get_logger(__name__).warning("self_healed", source=source, file_name=file_name)
        action = RunFileAction.SELF_HEALED
    last_modified = response.headers.get("last-modified")
    http_last_modified = parse_http_date(last_modified) if last_modified else None
    version = None
    # A crash-rerun finds its blob already recorded; only a first landing inserts a row.
    if is_new_blob:
        contract = contract_for_file(file_name)
        version = FileVersion(
            source=source,
            file_name=file_name,
            sha256=sha256,
            file_role=contract.role if contract else None,
            landing_path=str(landed.path),
            size_bytes=landed.size,
            line_count=line_count,
            http_last_modified=http_last_modified,
            request_url=url,
            content_type=response.headers.get("content-type", ""),
            header_ok=header.ok,
            header_observed="\t".join(header.observed),
            header_diff=header.diff,
            first_seen_run_id=ctx.run_id,
            first_seen_at=ctx.now(),
        )
        ctx.store.insert_version(version)
    if not header.ok:
        error = f"BAD_HEADER: {header.diff}"
        return FileOutcome(
            run_file(
                ctx,
                source,
                file_name,
                RunFileAction.FAILED,
                state,
                sha256_after=sha256,
                http_status=response.status,
                size=landed.size,
                error=error,
            ),
            version=version,
        )
    unchanged = state is not None and state.current_sha256 == sha256
    next_state = FileState(
        source=source,
        file_name=file_name,
        current_sha256=sha256,
        status=FileStatus.ACTIVE,
        consecutive_missing=0,
        listing_size=remote.size if remote else None,
        listing_ts_raw=remote.listing_ts_raw if remote else None,
        http_last_modified=http_last_modified,
        last_checked_run_id=ctx.run_id,
        last_changed_run_id=state.last_changed_run_id if unchanged and state else ctx.run_id,
        missing_since_run_id=None,
        updated_at=ctx.now(),
    )
    return FileOutcome(
        run_file(
            ctx,
            source,
            file_name,
            action,
            state,
            sha256_after=sha256,
            http_status=response.status,
            size=landed.size,
        ),
        version=version,
        next_state=next_state,
    )
