"""DataUSA sync: one GET, shape validation before landing, change detection by SHA-256."""

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import Any

from quest.config import IngestConfig
from quest.contracts.header import HeaderCheck, check_columns
from quest.contracts.loader import contract_by_role
from quest.contracts.models import FileContract
from quest.ingest.http import HttpClient
from quest.ingest.models import FileState
from quest.ingest.ops_store import OpsStore
from quest.ingest.sync import (
    FileOutcome,
    FileRejectedError,
    SyncContext,
    SyncResult,
    accept_body,
    check_length,
    guarded,
)

SOURCE = "datausa"
# The API has no file name, so the response lands under this logical name.
LOGICAL_FILE_NAME = "acs_population_nation.json"


class DataUsaShapeError(FileRejectedError):
    """The DataUSA response is not the JSON shape the population contract expects."""


@dataclass(frozen=True)
class DataUsaShape:
    """What validation learned about a body: its column check and record count."""

    header: HeaderCheck
    record_count: int


def validate_datausa_body(body: bytes, contract: FileContract) -> DataUsaShape:
    """Check a DataUSA body before it lands.

    Raises:
        DataUsaShapeError: If the body is not a JSON object, ``data`` is missing or empty,
            ``page.total`` differs from ``len(data)``, or ``columns`` differ from the contract.
    """
    try:
        parsed: Any = json.loads(body)
    except ValueError as exc:  # JSONDecodeError and UnicodeDecodeError are both ValueErrors
        raise DataUsaShapeError(f"body is not JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise DataUsaShapeError(f"body is a JSON {type(parsed).__name__}, expected an object")
    data = parsed.get("data")
    if not isinstance(data, list) or not data:
        raise DataUsaShapeError("data is missing or empty")
    total = (parsed.get("page") or {}).get("total")
    if total != len(data):
        raise DataUsaShapeError(f"page.total is {total!r} but data has {len(data)} records")
    header = check_columns(contract, [str(c) for c in parsed.get("columns") or []])
    if not header.ok:
        raise DataUsaShapeError(f"columns differ from the contract: {header.diff}")
    return DataUsaShape(header=header, record_count=len(data))


def sync_datausa(
    client: HttpClient, store: OpsStore, cfg: IngestConfig, now: Callable[[], datetime]
) -> SyncResult:
    """Fetch the population response and land it if its bytes changed."""
    ctx = SyncContext(store=store, landing_root=cfg.landing_root, run_id=cfg.run_id, now=now)
    state = store.file_states(SOURCE).get(LOGICAL_FILE_NAME)
    step = partial(_sync, client, ctx, cfg.datausa_url, state)
    return SyncResult.from_outcomes([guarded(ctx, SOURCE, LOGICAL_FILE_NAME, state, step)])


def _sync(client: HttpClient, ctx: SyncContext, url: str, state: FileState | None) -> FileOutcome:
    # No If-Modified-Since: DataUSA sends no Last-Modified, so every run GETs and compares hashes.
    response = client.get(url)
    check_length(response, None)
    shape = validate_datausa_body(response.body, contract_by_role("population"))
    return accept_body(
        ctx,
        source=SOURCE,
        file_name=LOGICAL_FILE_NAME,
        url=url,
        response=response,
        state=state,
        header=shape.header,
        line_count=shape.record_count,
        remote=None,
    )
