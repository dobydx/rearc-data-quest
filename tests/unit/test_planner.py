from dataclasses import replace

import pytest

from quest.ingest.models import FileState, FileStatus, RunFileAction
from quest.ingest.planner import (
    FetchDecision,
    ListingTooSmallError,
    PipelineUpdateInfo,
    absent_files,
    apply_absence,
    classify_download,
    code_changed,
    plan_listing,
    should_run_pipeline,
    state_changed,
)
from tests.unit.factories import LAST_MODIFIED, NOW, remote, state


def _exists(_: FileState) -> bool:
    return True


def _plan_one(listed_ts: str, stored: FileState | None, exists: bool = True) -> FetchDecision:
    states = {stored.file_name: stored} if stored else {}
    (planned,) = plan_listing([remote("pr.series", ts=listed_ts)], states, lambda _: exists, 0.5)
    return planned.decision


def test_new_file_is_fetched_without_ims() -> None:
    (planned,) = plan_listing([remote("pr.series")], {}, _exists, 0.5)

    assert planned.decision is FetchDecision.FETCH
    assert planned.if_modified_since is None


def test_same_fingerprint_is_skipped() -> None:
    assert _plan_one("9/3/2026 7:30 AM", state("pr.series")) is FetchDecision.SKIP


def test_size_change_is_fetched_without_ims() -> None:
    # A revert can restore an older file, so a 304 would hide the change.
    (planned,) = plan_listing(
        [remote("pr.series", size=101)], {"pr.series": state("pr.series")}, _exists, 0.5
    )

    assert planned.decision is FetchDecision.FETCH
    assert planned.if_modified_since is None


def test_size_and_time_change_is_fetched_without_ims() -> None:
    stored = state("pr.series", listing_ts_raw="9/3/2026 8:30 AM")

    (planned,) = plan_listing(
        [remote("pr.series", size=101, ts="9/3/2026 7:30 AM")], {"pr.series": stored}, _exists, 0.5
    )

    assert planned.if_modified_since is None


def test_dst_shifted_listing_time_fetches_with_ims() -> None:
    stored = state("pr.series", listing_ts_raw="9/3/2026 8:30 AM")

    (planned,) = plan_listing(
        [remote("pr.series", ts="9/3/2026 7:30 AM")], {"pr.series": stored}, _exists, 0.5
    )

    assert planned.decision is FetchDecision.FETCH
    assert planned.if_modified_since == LAST_MODIFIED


def test_missing_landed_file_triggers_self_heal() -> None:
    (planned,) = plan_listing(
        [remote("pr.series")], {"pr.series": state("pr.series")}, lambda _: False, 0.5
    )

    assert planned.decision is FetchDecision.FETCH_SELF_HEAL
    assert planned.if_modified_since is None


def test_empty_listing_raises() -> None:
    with pytest.raises(ListingTooSmallError):
        plan_listing([], {}, _exists, 0.5)


def test_listing_below_half_of_active_raises() -> None:
    states = {f"f{i}": state(f"f{i}") for i in range(12)}
    listing = [remote(f"f{i}") for i in range(5)]

    with pytest.raises(ListingTooSmallError, match="5"):
        plan_listing(listing, states, _exists, 0.5)


def test_listing_at_half_of_active_is_accepted() -> None:
    states = {f"f{i}": state(f"f{i}") for i in range(12)}

    assert len(plan_listing([remote(f"f{i}") for i in range(6)], states, _exists, 0.5)) == 6


def test_absence_debounce_active_to_missing_to_deleted() -> None:
    states = {"pr.series": state("pr.series"), "pr.class": state("pr.class")}

    assert absent_files([remote("pr.class")], states) == ["pr.series"]
    missing = apply_absence(states["pr.series"], "r1", NOW)
    assert (missing.status, missing.consecutive_missing) == (FileStatus.MISSING, 1)
    assert missing.missing_since_run_id == "r1"
    assert missing.current_sha256 == states["pr.series"].current_sha256

    assert absent_files([remote("pr.class")], {**states, "pr.series": missing}) == ["pr.series"]
    deleted = apply_absence(missing, "r2", NOW)
    assert (deleted.status, deleted.consecutive_missing) == (FileStatus.DELETED, 2)
    assert deleted.missing_since_run_id == "r1"
    assert deleted.last_checked_run_id == "r2"


def test_deleted_files_are_not_absent_again() -> None:
    states = {"pr.series": state("pr.series", status=FileStatus.DELETED)}

    assert absent_files([remote("pr.class")], states) == []


def test_apply_absence_rejects_deleted_state() -> None:
    with pytest.raises(ValueError, match="deleted"):
        apply_absence(state("pr.series", status=FileStatus.DELETED), "r1", NOW)


def test_deleted_file_reappearing_is_fetched_as_new() -> None:
    stored = state("pr.series", status=FileStatus.DELETED, consecutive_missing=2)

    (planned,) = plan_listing([remote("pr.series")], {"pr.series": stored}, _exists, 0.5)

    assert planned.decision is FetchDecision.FETCH
    assert planned.if_modified_since is None


def test_classify_same_hash() -> None:
    stored = state("pr.series", current_sha256="b" * 64)

    assert classify_download(stored, {"b" * 64}, "b" * 64) is RunFileAction.SAME_HASH


def test_classify_revert_a_b_a() -> None:
    stored = state("pr.series", current_sha256="b" * 64)

    action = classify_download(stored, {"a" * 64, "b" * 64}, "a" * 64)

    assert action is RunFileAction.REVERTED_TO_KNOWN_VERSION


def test_classify_new_version() -> None:
    stored = state("pr.series", current_sha256="b" * 64)

    assert classify_download(stored, {"b" * 64}, "c" * 64) is RunFileAction.NEW_VERSION


def test_classify_first_pointer_is_new_even_if_blob_known() -> None:
    # A crash after landing but before the pointer flip leaves a known blob and no state.
    assert classify_download(None, {"a" * 64}, "a" * 64) is RunFileAction.NEW_VERSION


POINTER = ("pr.series", "a" * 64)
DONE = PipelineUpdateInfo(state="COMPLETED", code_version="1.0")


def _gate(**overrides: object) -> bool:
    args: dict[str, object] = {
        "pointers": {POINTER},
        "silver_lineage": {POINTER},
        "last_update": DONE,
        "code_version": "1.0",
        "force": False,
    }
    args.update(overrides)
    return should_run_pipeline(**args)  # type: ignore[arg-type]


def test_gate_true_when_pointer_not_in_silver() -> None:
    assert _gate(pointers={POINTER, ("pr.class", "c" * 64)})


def test_gate_true_when_last_update_failed() -> None:
    assert _gate(last_update=PipelineUpdateInfo(state="FAILED", code_version="1.0"))


def test_gate_true_when_no_update_recorded() -> None:
    assert _gate(last_update=None)


def test_gate_true_when_code_version_changed() -> None:
    assert _gate(code_version="1.1")


def test_gate_true_when_forced() -> None:
    assert _gate(force=True)


def test_gate_true_when_silver_missing() -> None:
    assert _gate(silver_lineage=None)


def test_gate_true_when_served_version_was_deleted() -> None:
    deleted_lookup = ("pr.footnote", "f" * 64)

    assert _gate(silver_lineage={POINTER, deleted_lookup})


def test_gate_false_when_nothing_pending() -> None:
    assert not _gate()


def test_state_changed_ignores_check_bookkeeping() -> None:
    before = state("pr.series")

    assert not state_changed(before, replace(before, last_checked_run_id="r9", updated_at=NOW))
    assert state_changed(before, replace(before, listing_ts_raw="9/3/2026 8:30 AM"))
    assert state_changed(before, replace(before, status=FileStatus.MISSING))
    assert state_changed(before, replace(before, consecutive_missing=1))
    assert state_changed(before, replace(before, http_last_modified=NOW))
    assert state_changed(None, before)


@pytest.mark.parametrize(
    ("last_update", "expected"),
    [
        (None, True),
        (PipelineUpdateInfo(state="COMPLETED", code_version="1.0"), False),
        (PipelineUpdateInfo(state="FAILED", code_version="1.0"), False),
        (PipelineUpdateInfo(state="COMPLETED", code_version="0.9"), True),
    ],
)
def test_code_changed_compares_the_last_update_code_version(
    last_update: PipelineUpdateInfo | None, expected: bool
) -> None:
    assert code_changed(last_update, "1.0") is expected
