from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from quest.config import DEFAULT_DATAUSA_URL, IngestConfig
from quest.ingest.models import FileState, FileStatus, RemoteFile

BASE_URL = "https://download.bls.gov/pub/time.series/pr/"
LAST_MODIFIED = datetime(2026, 9, 3, 12, 30, tzinfo=UTC)
NOW = datetime(2026, 11, 3, 15, 0, tzinfo=UTC)


def remote(name: str, size: int = 100, ts: str = "9/3/2026 7:30 AM") -> RemoteFile:
    return RemoteFile(name=name, url=f"{BASE_URL}{name}", size=size, listing_ts_raw=ts)


def state(name: str, **overrides: object) -> FileState:
    base = FileState(
        source="bls_pr",
        file_name=name,
        current_sha256="a" * 64,
        status=FileStatus.ACTIVE,
        consecutive_missing=0,
        listing_size=100,
        listing_ts_raw="9/3/2026 7:30 AM",
        http_last_modified=LAST_MODIFIED,
        last_checked_run_id="r0",
        last_changed_run_id="r0",
        missing_since_run_id=None,
        updated_at=LAST_MODIFIED,
    )
    return replace(base, **overrides)  # type: ignore[arg-type]


def make_config(landing_root: Path = Path("/unused"), **overrides: object) -> IngestConfig:
    values: dict[str, object] = {
        "catalog": "workspace",
        "landing_root": landing_root,
        "bls_base_url": BASE_URL,
        "datausa_url": DEFAULT_DATAUSA_URL,
        "contact_email": "a@b.c",
        "run_id": "r1",
    }
    values.update(overrides)
    return IngestConfig(**values)  # type: ignore[arg-type]
