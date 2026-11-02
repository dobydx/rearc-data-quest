"""Pure ingest decisions: what to fetch, how to classify a download, whether to run the pipeline."""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum

from quest.ingest.models import FileState, FileStatus, RemoteFile, RunFileAction


class ListingTooSmallError(Exception):
    """Raised when the listing is empty or far smaller than the files we track."""


class FetchDecision(StrEnum):
    """What to do with one listed file."""

    SKIP = "SKIP"
    FETCH = "FETCH"
    FETCH_SELF_HEAL = "FETCH_SELF_HEAL"


@dataclass(frozen=True)
class PlannedFile:
    """A listed file with its decision and the If-Modified-Since to send, if any."""

    remote: RemoteFile
    decision: FetchDecision
    if_modified_since: datetime | None


@dataclass(frozen=True)
class PipelineUpdateInfo:
    """Outcome and code version of the last pipeline update."""

    state: str
    code_version: str | None


def plan_listing(
    listing: Sequence[RemoteFile],
    states: Mapping[str, FileState],
    current_path_exists: Callable[[FileState], bool],
    min_active_ratio: float,
) -> list[PlannedFile]:
    """Decide, per listed file, whether to skip it, fetch it, or re-fetch a lost landing.

    Raises:
        ListingTooSmallError: If the listing is empty or lists fewer than
            ``min_active_ratio`` of the active files, which points at a broken listing.
    """
    active = sum(1 for s in states.values() if s.status is FileStatus.ACTIVE)
    if not listing or len(listing) < min_active_ratio * active:
        raise ListingTooSmallError(
            f"listing has {len(listing)} files, {active} are active "
            f"(minimum ratio {min_active_ratio})"
        )
    return [_plan_file(remote, states.get(remote.name), current_path_exists) for remote in listing]


def _plan_file(
    remote: RemoteFile,
    state: FileState | None,
    current_path_exists: Callable[[FileState], bool],
) -> PlannedFile:
    if state is None or state.status is FileStatus.DELETED:
        return PlannedFile(remote, FetchDecision.FETCH, None)
    if not current_path_exists(state):
        # No IMS: a 304 would leave the Volume without the bytes silver points at.
        return PlannedFile(remote, FetchDecision.FETCH_SELF_HEAL, None)
    if state.listing_size != remote.size:
        # No IMS: a revert can restore an older file, and its Last-Modified would earn a 304.
        return PlannedFile(remote, FetchDecision.FETCH, None)
    if state.listing_ts_raw == remote.listing_ts_raw:
        return PlannedFile(remote, FetchDecision.SKIP, None)
    # Same size, new time string: the DST shift, where a 304 is the expected answer.
    return PlannedFile(remote, FetchDecision.FETCH, state.http_last_modified)


def absent_files(listing: Sequence[RemoteFile], states: Mapping[str, FileState]) -> list[str]:
    """Return tracked active or missing files that the listing no longer shows."""
    listed = {remote.name for remote in listing}
    return sorted(
        name
        for name, state in states.items()
        if state.status in (FileStatus.ACTIVE, FileStatus.MISSING) and name not in listed
    )


def apply_absence(state: FileState, run_id: str, now: datetime) -> FileState:
    """Debounce a deletion: the first absence marks the file missing, the second deleted."""
    if state.status is FileStatus.DELETED:
        raise ValueError(f"{state.file_name} is already deleted")
    if state.status is FileStatus.ACTIVE:
        return replace(
            state,
            status=FileStatus.MISSING,
            consecutive_missing=1,
            missing_since_run_id=run_id,
            last_checked_run_id=run_id,
            updated_at=now,
        )
    return replace(
        state,
        status=FileStatus.DELETED,
        consecutive_missing=2,
        last_checked_run_id=run_id,
        last_changed_run_id=run_id,
        updated_at=now,
    )


def classify_download(state: FileState | None, served_shas: set[str], sha256: str) -> RunFileAction:
    """Classify a downloaded blob against the current pointer and every version once served.

    ``served_shas`` holds versions a committed pointer flip made current. A blob that only
    landed (its run crashed before the flip) was never served, so it counts as new.
    """
    if state is not None and sha256 == state.current_sha256:
        return RunFileAction.SAME_HASH
    if state is not None and sha256 in served_shas:
        return RunFileAction.REVERTED_TO_KNOWN_VERSION
    return RunFileAction.NEW_VERSION


# What a state means for silver; run bookkeeping (last_checked_run_id, updated_at) is not here.
_STATE_CONTENT = (
    "current_sha256",
    "status",
    "listing_size",
    "listing_ts_raw",
    "http_last_modified",
    "consecutive_missing",
)


def state_changed(before: FileState | None, after: FileState) -> bool:
    """True when ``after`` differs from ``before`` in anything but run bookkeeping."""
    return before is None or any(
        getattr(before, name) != getattr(after, name) for name in _STATE_CONTENT
    )


def code_changed(last_update: PipelineUpdateInfo | None, code_version: str) -> bool:
    """Return True when no pipeline update ran ``code_version``, so tables may legitimately move."""
    return last_update is None or last_update.code_version != code_version


def should_run_pipeline(
    pointers: set[tuple[str, str]],
    silver_lineage: set[tuple[str, str]] | None,
    last_update: PipelineUpdateInfo | None,
    code_version: str,
    force: bool,
) -> bool:
    """Return True when silver may be stale or the run is forced.

    Stale means pointers and lineage differ, or the last update failed or ran other code.

    ``pointers`` and ``silver_lineage`` hold ``(file_name, sha256)`` pairs. A pointer missing
    from silver is a pending version; a served version with no pointer is a deletion. A None
    lineage means silver does not exist yet.
    """
    return (
        force
        or silver_lineage is None
        or pointers != silver_lineage
        or code_changed(last_update, code_version)
        or last_update is None
        or last_update.state != "COMPLETED"
    )
