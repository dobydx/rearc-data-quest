"""BLS sync: listing, fingerprint skip, conditional GET, landing, absence debounce."""

from collections.abc import Callable
from dataclasses import replace
from datetime import datetime
from functools import partial

from quest.config import IngestConfig
from quest.contracts.header import HeaderCheck, check_header
from quest.contracts.loader import contract_for_file
from quest.ingest.http import HttpClient
from quest.ingest.landing import count_data_lines, landing_path
from quest.ingest.listing import parse_listing
from quest.ingest.models import FileState, FileStatus, RemoteFile, RunFileAction
from quest.ingest.ops_store import OpsStore
from quest.ingest.planner import (
    FetchDecision,
    PlannedFile,
    absent_files,
    apply_absence,
    plan_listing,
)
from quest.ingest.sync import (
    FileOutcome,
    SyncContext,
    SyncResult,
    accept_body,
    check_length,
    guarded,
    run_file,
)

SOURCE = "bls_pr"
# Free-text and unmodelled files have no header contract, so they always pass.
_NO_HEADER = HeaderCheck(ok=True, observed=(), diff=None)


def sync_bls(
    client: HttpClient, store: OpsStore, cfg: IngestConfig, now: Callable[[], datetime]
) -> SyncResult:
    """Sync every file in the BLS listing and debounce the ones that disappeared.

    Raises:
        ListingTooSmallError: If the listing looks broken; nothing is fetched.
        ForbiddenError: If BLS refuses the User-Agent; the run stops at once.
    """
    ctx = SyncContext(store=store, landing_root=cfg.landing_root, run_id=cfg.run_id, now=now)
    listing_html = client.get(cfg.bls_base_url).body.decode("utf-8", errors="replace")
    listing = parse_listing(listing_html, cfg.bls_base_url)
    states = store.file_states(SOURCE)
    plan = plan_listing(listing, states, _current_path_exists(cfg), cfg.min_active_ratio)
    outcomes = []
    for planned in plan:
        state = states.get(planned.remote.name)
        step = partial(_sync_file, client, ctx, planned, state)
        outcomes.append(guarded(ctx, SOURCE, planned.remote.name, state, step))
    outcomes += [_absent(ctx, states[name]) for name in absent_files(listing, states)]
    return SyncResult.from_outcomes(outcomes, files_listed=len(listing))


def _current_path_exists(cfg: IngestConfig) -> Callable[[FileState], bool]:
    def exists(state: FileState) -> bool:
        path = landing_path(cfg.landing_root, SOURCE, state.file_name, state.current_sha256)
        return path.exists()

    return exists


def _sync_file(
    client: HttpClient, ctx: SyncContext, planned: PlannedFile, state: FileState | None
) -> FileOutcome:
    remote = planned.remote
    if planned.decision is FetchDecision.SKIP and state is not None:
        return FileOutcome(
            run_file(
                ctx,
                SOURCE,
                remote.name,
                RunFileAction.SKIPPED_FINGERPRINT,
                state,
                sha256_after=state.current_sha256,
            ),
            next_state=_seen(ctx, state, remote),
        )
    response = client.get(remote.url, if_modified_since=planned.if_modified_since)
    if response.status == 304 and state is not None:
        return FileOutcome(
            run_file(
                ctx,
                SOURCE,
                remote.name,
                RunFileAction.NOT_MODIFIED,
                state,
                sha256_after=state.current_sha256,
                http_status=304,
            ),
            next_state=_seen(ctx, state, remote),
        )
    check_length(response, remote.size)
    contract = contract_for_file(remote.name)
    header = (
        check_header(contract, response.body)
        if contract is not None and contract.fmt == "tsv"
        else _NO_HEADER
    )
    return accept_body(
        ctx,
        source=SOURCE,
        file_name=remote.name,
        url=remote.url,
        response=response,
        state=state,
        header=header,
        line_count=count_data_lines(response.body),
        remote=remote,
    )


def _seen(ctx: SyncContext, state: FileState, remote: RemoteFile) -> FileState:
    """Keep the pointer, refresh the fingerprint, and clear a pending absence."""
    return replace(
        state,
        status=FileStatus.ACTIVE,
        consecutive_missing=0,
        missing_since_run_id=None,
        listing_size=remote.size,
        listing_ts_raw=remote.listing_ts_raw,
        last_checked_run_id=ctx.run_id,
        updated_at=ctx.now(),
    )


def _absent(ctx: SyncContext, state: FileState) -> FileOutcome:
    next_state = apply_absence(state, ctx.run_id, ctx.now())
    action = (
        RunFileAction.DELETED if next_state.status is FileStatus.DELETED else RunFileAction.MISSING
    )
    return FileOutcome(
        run_file(ctx, SOURCE, state.file_name, action, state, sha256_after=state.current_sha256),
        next_state=next_state,
    )
