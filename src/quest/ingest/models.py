"""Ingest records. Each dataclass mirrors one ``quest_ops`` table row; times are aware UTC."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class FileStatus(StrEnum):
    """Lifecycle of a logical file in ``file_state``."""

    ACTIVE = "active"
    MISSING = "missing"
    DELETED = "deleted"


# A missing file keeps serving its last version until a second absence deletes it.
SERVED_STATUSES = (FileStatus.ACTIVE, FileStatus.MISSING)


class RunStatus(StrEnum):
    """Status of one ingest run in ``ingest_runs``."""

    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class RunFileAction(StrEnum):
    """What one run did with one file, recorded in ``ingest_run_files``."""

    SKIPPED_FINGERPRINT = "SKIPPED_FINGERPRINT"
    NOT_MODIFIED = "NOT_MODIFIED"
    SAME_HASH = "SAME_HASH"
    NEW_VERSION = "NEW_VERSION"
    REVERTED_TO_KNOWN_VERSION = "REVERTED_TO_KNOWN_VERSION"
    SELF_HEALED = "SELF_HEALED"
    MISSING = "MISSING"
    DELETED = "DELETED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class RemoteFile:
    """One file row of the BLS directory listing."""

    name: str
    url: str
    size: int
    # Server-local time with no zone; compared as text, never parsed.
    listing_ts_raw: str


@dataclass(frozen=True)
class FileState:
    """Current pointer for one logical file (``quest_ops.file_state``)."""

    source: str
    file_name: str
    current_sha256: str
    status: FileStatus
    consecutive_missing: int
    listing_size: int | None
    listing_ts_raw: str | None
    http_last_modified: datetime | None
    last_checked_run_id: str
    last_changed_run_id: str
    missing_since_run_id: str | None
    updated_at: datetime


@dataclass(frozen=True)
class FileVersion:
    """One immutable landed blob (``quest_ops.file_versions``)."""

    source: str
    file_name: str
    sha256: str
    file_role: str | None
    landing_path: str
    size_bytes: int
    line_count: int
    http_last_modified: datetime | None
    request_url: str
    content_type: str
    header_ok: bool
    header_observed: str
    header_diff: str | None
    first_seen_run_id: str
    first_seen_at: datetime


@dataclass(frozen=True)
class RunFile:
    """What one run did with one file (``quest_ops.ingest_run_files``)."""

    run_id: str
    source: str
    file_name: str
    action: RunFileAction
    sha256_before: str | None
    sha256_after: str | None
    pointer_flipped: bool
    http_status: int | None
    bytes: int | None
    elapsed_ms: int
    error: str | None
    recorded_at: datetime


@dataclass(frozen=True)
class IngestRun:
    """One ingest task run and its counts (``quest_ops.ingest_runs``)."""

    run_id: str
    job_id: str | None
    job_run_id: str | None
    started_at: datetime
    status: RunStatus
    code_version: str
    trigger: str
    params_json: str
    ended_at: datetime | None = None
    files_listed: int = 0
    files_skipped_fingerprint: int = 0
    files_not_modified: int = 0
    files_same_hash: int = 0
    versions_landed: int = 0
    pointers_flipped: int = 0
    files_missing: int = 0
    files_deleted: int = 0
    files_self_healed: int = 0
    files_failed: int = 0
    error_class: str | None = None
    error_message: str | None = None
