from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import format_datetime
from pathlib import Path

import httpx
import pytest
import respx

from quest.config import DEFAULT_DATAUSA_URL
from quest.ingest.datausa import LOGICAL_FILE_NAME
from quest.ingest.http import ForbiddenError, HttpClient, parse_http_date
from quest.ingest.landing import landing_path, sha256_hex
from quest.ingest.main import IngestFailedError, run_ingest
from quest.ingest.models import FileState, FileStatus, IngestRun, RunFileAction, RunStatus
from quest.ingest.ops_store import InMemoryOpsStore
from quest.ingest.planner import ListingTooSmallError
from tests.unit.factories import BASE_URL, NOW, make_config

LISTING_TS = "9/3/2026 7:30 AM"
FIRST_RELEASE = datetime(2026, 9, 3, 12, 30, tzinfo=UTC)
NEXT_RELEASE = datetime(2026, 11, 5, 13, 30, tzinfo=UTC)


@dataclass
class FakeBls:
    """Serves the fixture files the way download.bls.gov does, including If-Modified-Since."""

    files: dict[str, bytes]
    last_modified: dict[str, datetime] = field(default_factory=dict)
    listing_ts: dict[str, str] = field(default_factory=dict)
    listed_size: dict[str, int] = field(default_factory=dict)
    hidden: set[str] = field(default_factory=set)
    calls: Counter[str] = field(default_factory=Counter)

    def publish(self, name: str, body: bytes, ts: str = "11/5/2026 8:30 AM") -> None:
        self.files[name] = body
        self.last_modified[name] = NEXT_RELEASE
        self.listing_ts[name] = ts

    def listing_html(self) -> str:
        rows = "".join(
            f"{self.listing_ts.get(name, LISTING_TS)} "
            f"{self.listed_size.get(name, len(body))} "
            f'<A HREF="/pub/time.series/pr/{name}">{name}</A><br>'
            for name, body in sorted(self.files.items())
            if name not in self.hidden
        )
        return f'<pre><A HREF="/pub/time.series/">[To Parent Directory]</A><br><br>{rows}</pre>'

    def handle(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == BASE_URL:
            return httpx.Response(200, text=self.listing_html())
        name = url.removeprefix(BASE_URL)
        self.calls[name] += 1
        modified = self.last_modified.get(name, FIRST_RELEASE)
        ims = request.headers.get("if-modified-since")
        if ims and parse_http_date(ims) >= modified:
            return httpx.Response(304)
        body = self.files[name]
        return httpx.Response(
            200,
            content=body,
            headers={
                "last-modified": format_datetime(modified, usegmt=True),
                "content-length": str(len(body)),
                "content-type": "application/octet-stream",
            },
        )


@dataclass
class World:
    bls: FakeBls
    datausa_body: bytes
    store: InMemoryOpsStore
    landing_root: Path
    datausa_status: int = 200
    runs: int = 0

    def run(self, **overrides: object) -> IngestRun:
        self.runs += 1
        cfg = make_config(self.landing_root, run_id=f"r{self.runs}", **overrides)
        with (
            respx.mock(assert_all_called=False) as router,
            HttpClient("ua", 10.0, 60.0, 2, 1.0, sleep=lambda _: None) as client,
        ):
            router.get(url__startswith=BASE_URL).mock(side_effect=self.bls.handle)
            router.get(DEFAULT_DATAUSA_URL).mock(
                side_effect=lambda _: httpx.Response(self.datausa_status, content=self.datausa_body)
            )
            return run_ingest(cfg, client, self.store, now=lambda: NOW)

    def states(self, source: str = "bls_pr") -> dict[str, FileState]:
        return self.store.file_states(source)

    def actions(self, run_id: str) -> Counter[RunFileAction]:
        return Counter(
            rf.action for (rid, _, _), rf in self.store.run_files.items() if rid == run_id
        )


@pytest.fixture
def world(fixtures_dir: Path, tmp_path: Path) -> World:
    files = {path.name: path.read_bytes() for path in (fixtures_dir / "bls").iterdir()}
    return World(
        bls=FakeBls(files=files),
        datausa_body=(fixtures_dir / "datausa_population.json").read_bytes(),
        store=InMemoryOpsStore(),
        landing_root=tmp_path / "landing",
    )


def test_first_run_lands_all_and_flips_pointers(world: World) -> None:
    run = world.run()

    assert run.status is RunStatus.SUCCEEDED
    assert len(world.store.versions) == 13
    assert world.actions("r1") == {RunFileAction.NEW_VERSION: 13}
    states = {**world.states(), **world.states("datausa")}
    assert len(states) == 13
    assert {s.status for s in states.values()} == {FileStatus.ACTIVE}
    for name, body in world.bls.files.items():
        assert states[name].current_sha256 == sha256_hex(body)
        path = landing_path(world.landing_root, "bls_pr", name, sha256_hex(body))
        assert path.read_bytes() == body
    assert run.versions_landed == 13
    assert run.pointers_flipped == 13
    assert run.files_listed == 12
    assert all(rf.pointer_flipped for rf in world.store.run_files.values())


def test_run_audit_records_the_configured_code_version(world: World) -> None:
    # The synced workspace folder has no .git, so the package version is always 0.0.0 there.
    run = world.run(code_version="git-abc1234")

    assert run.code_version == "git-abc1234"
    assert world.store.runs[run.run_id].code_version == "git-abc1234"


def test_second_run_unchanged_downloads_nothing(world: World) -> None:
    world.run()
    world.bls.calls.clear()

    run = world.run()

    assert run.status is RunStatus.SUCCEEDED
    assert world.actions("r2") == {
        RunFileAction.SKIPPED_FINGERPRINT: 12,
        RunFileAction.SAME_HASH: 1,
    }
    assert sum(world.bls.calls.values()) == 0
    assert len(world.store.versions) == 13
    assert run.pointers_flipped == 0


def test_dst_day_uses_304(world: World) -> None:
    world.run()
    world.bls.listing_ts = dict.fromkeys(world.bls.files, "9/3/2026 8:30 AM")

    run = world.run()

    assert world.actions("r2")[RunFileAction.NOT_MODIFIED] == 12
    assert len(world.store.versions) == 13
    assert run.files_not_modified == 12
    assert {s.listing_ts_raw for s in world.states().values()} == {"9/3/2026 8:30 AM"}


def test_revert_to_older_last_modified_with_new_size_is_fetched(world: World) -> None:
    world.run()
    older_body = world.bls.files["pr.class"] + b"9\tOther\t0\tT\t3\r\n"
    world.bls.files["pr.class"] = older_body
    world.bls.last_modified["pr.class"] = FIRST_RELEASE
    world.bls.listing_ts["pr.class"] = "9/3/2026 7:30 AM"

    world.run()

    assert world.actions("r2")[RunFileAction.NEW_VERSION] == 1
    assert world.actions("r2")[RunFileAction.NOT_MODIFIED] == 0
    assert world.states()["pr.class"].current_sha256 == sha256_hex(older_body)


def test_changed_file_lands_new_version(world: World) -> None:
    world.run()
    new_series = world.bls.files["pr.series"] + b"PRS99999999\t\r\n"
    world.bls.publish("pr.series", new_series)

    run = world.run()

    assert world.actions("r2")[RunFileAction.NEW_VERSION] == 1
    state = world.states()["pr.series"]
    assert state.current_sha256 == sha256_hex(new_series)
    assert state.http_last_modified == NEXT_RELEASE
    assert state.last_changed_run_id == "r2"
    assert run.pointers_flipped == 1


def test_revert_flips_pointer_back(world: World) -> None:
    world.run()
    original = world.bls.files["pr.class"]
    world.bls.publish("pr.class", original + b"9\tOther\t0\tT\t3\r\n", ts="11/5/2026 8:30 AM")
    world.run()
    world.bls.publish("pr.class", original, ts="11/6/2026 8:30 AM")
    world.bls.last_modified["pr.class"] = datetime(2026, 11, 6, tzinfo=UTC)

    world.run()

    assert world.actions("r3")[RunFileAction.REVERTED_TO_KNOWN_VERSION] == 1
    assert world.states()["pr.class"].current_sha256 == sha256_hex(original)
    assert len(world.store.known_shas("bls_pr", "pr.class")) == 2


def test_length_mismatch_fails_run_without_flip(world: World) -> None:
    world.run()
    before = world.states()
    world.bls.publish("pr.class", world.bls.files["pr.class"] + b"9\tOther\t0\tT\t3\r\n")
    world.bls.listed_size["pr.class"] = 5

    with pytest.raises(IngestFailedError, match=r"pr\.class"):
        world.run()

    assert world.states() == before
    failed = world.store.run_files[("r2", "bls_pr", "pr.class")]
    assert failed.action is RunFileAction.FAILED
    assert failed.error is not None
    assert "LengthMismatchError" in failed.error
    assert len(world.store.known_shas("bls_pr", "pr.class")) == 1
    assert world.store.runs["r2"].status is RunStatus.FAILED
    assert not any(
        rf.pointer_flipped for (rid, _, _), rf in world.store.run_files.items() if rid == "r2"
    )


def test_header_drift_lands_but_blocks_flip(world: World) -> None:
    world.run()
    before = world.states()
    drifted = world.bls.files["pr.series"].replace(b"base_year", b"base_period", 1)
    world.bls.publish("pr.series", drifted)

    with pytest.raises(IngestFailedError, match=r"pr\.series"):
        world.run()

    version = world.store.versions[("bls_pr", "pr.series", sha256_hex(drifted))]
    assert not version.header_ok
    assert version.header_diff is not None
    assert "base_period" in version.header_diff
    assert landing_path(world.landing_root, "bls_pr", "pr.series", sha256_hex(drifted)).exists()
    assert world.states() == before
    run_file = world.store.run_files[("r2", "bls_pr", "pr.series")]
    assert run_file.error is not None
    assert run_file.error.startswith("BAD_HEADER")


def test_datausa_html_body_fails_before_landing(world: World) -> None:
    world.datausa_body = b"<html>maintenance</html>"

    with pytest.raises(IngestFailedError, match=LOGICAL_FILE_NAME):
        world.run()

    assert not (world.landing_root / "datausa").exists()
    assert world.states() == {}


def test_datausa_total_mismatch_fails(world: World) -> None:
    world.datausa_body = world.datausa_body.replace(b'"total":11', b'"total":12')

    with pytest.raises(IngestFailedError):
        world.run()

    failed = world.store.run_files[("r1", "datausa", LOGICAL_FILE_NAME)]
    assert failed.error is not None
    assert "page.total" in failed.error


def test_403_propagates_forbidden(world: World) -> None:
    world.bls.handle = lambda request: httpx.Response(403)  # type: ignore[method-assign]

    with pytest.raises(ForbiddenError):
        world.run()

    run = world.store.runs["r1"]
    assert run.status is RunStatus.FAILED
    assert run.error_class == "ForbiddenError"


def test_rerun_after_crash_is_idempotent(world: World, monkeypatch: pytest.MonkeyPatch) -> None:
    real_commit = world.store.commit_states
    crashes = iter([True])

    def crash_once(states: list[FileState]) -> None:
        if next(crashes, False):
            raise OSError("driver lost")
        real_commit(states)

    monkeypatch.setattr(world.store, "commit_states", crash_once)

    with pytest.raises(OSError, match="driver lost"):
        world.run()
    assert world.states() == {}
    assert world.store.runs["r1"].status is RunStatus.FAILED
    audit = [rf for (rid, _, _), rf in world.store.run_files.items() if rid == "r1"]
    assert len(audit) == 13
    assert not any(rf.pointer_flipped for rf in audit)

    run = world.run()

    assert run.status is RunStatus.SUCCEEDED
    assert len(world.store.versions) == 13
    assert run.versions_landed == 0
    for name, body in world.bls.files.items():
        assert world.states()[name].current_sha256 == sha256_hex(body)
        shas = list((world.landing_root / "bls_pr" / name).iterdir())
        assert [p.name for p in shas] == [f"sha256={sha256_hex(body)}"]
    assert run.pointers_flipped == 13


def test_file_absent_twice_becomes_deleted(world: World) -> None:
    world.run()
    world.bls.hidden.add("pr.contacts")

    world.run()
    assert world.states()["pr.contacts"].status is FileStatus.MISSING
    assert world.actions("r2")[RunFileAction.MISSING] == 1

    run = world.run()
    assert world.states()["pr.contacts"].status is FileStatus.DELETED
    assert world.actions("r3")[RunFileAction.DELETED] == 1
    assert run.files_deleted == 1
    assert run.pointers_flipped == 1


def test_missing_file_listed_again_is_active(world: World) -> None:
    world.run()
    world.bls.hidden.add("pr.contacts")
    world.run()
    world.bls.hidden.clear()

    world.run()

    state = world.states()["pr.contacts"]
    assert (state.status, state.consecutive_missing, state.missing_since_run_id) == (
        FileStatus.ACTIVE,
        0,
        None,
    )


def test_self_heal_relands_missing_file(world: World) -> None:
    world.run()
    state = world.states()["pr.series"]
    path = landing_path(world.landing_root, "bls_pr", "pr.series", state.current_sha256)
    path.unlink()

    run = world.run()

    assert world.actions("r2")[RunFileAction.SELF_HEALED] == 1
    assert path.read_bytes() == world.bls.files["pr.series"]
    assert run.files_self_healed == 1
    assert run.pointers_flipped == 0
    assert world.states()["pr.series"] == state


def test_listing_too_small_fails_run(world: World) -> None:
    world.run()
    world.bls.hidden.update(list(world.bls.files)[:8])

    with pytest.raises(ListingTooSmallError, match="listing has 4 files"):
        world.run()

    assert world.store.runs["r2"].status is RunStatus.FAILED


def test_unchanged_run_writes_no_state(world: World, monkeypatch: pytest.MonkeyPatch) -> None:
    world.run()
    committed: list[list[FileState]] = []
    real_commit = world.store.commit_states

    def recording_commit(states: list[FileState]) -> None:
        committed.append(list(states))
        real_commit(states)

    monkeypatch.setattr(world.store, "commit_states", recording_commit)

    world.run()

    assert committed in ([], [[]])
    assert {s.last_checked_run_id for s in world.states().values()} == {"r1"}


def test_unlanded_pointer_after_crash_is_new_not_revert(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    world.run()
    original = world.bls.files["pr.class"]
    changed = original + b"9\tOther\t0\tT\t3\r\n"
    world.bls.publish("pr.class", changed)
    real_commit = world.store.commit_states
    crashes = iter([True])

    def crash_once(states: list[FileState]) -> None:
        if next(crashes, False):
            raise OSError("driver lost")
        real_commit(states)

    monkeypatch.setattr(world.store, "commit_states", crash_once)
    with pytest.raises(OSError):
        world.run()

    run = world.run()

    assert world.actions("r3")[RunFileAction.NEW_VERSION] == 1
    assert world.actions("r3")[RunFileAction.REVERTED_TO_KNOWN_VERSION] == 0
    assert world.states()["pr.class"].current_sha256 == sha256_hex(changed)
    assert run.versions_landed == 0


def test_deleted_file_restored_with_the_same_bytes_flips_its_pointer(world: World) -> None:
    # SAME_HASH by content, but silver stopped serving it when it was deleted, so it re-enters.
    world.run()
    world.bls.hidden.add("pr.class")
    world.run()
    world.run()
    assert world.states()["pr.class"].status is FileStatus.DELETED
    world.bls.hidden.discard("pr.class")

    run = world.run()

    assert world.states()["pr.class"].status is FileStatus.ACTIVE
    restored = world.store.run_files[("r4", "bls_pr", "pr.class")]
    assert (restored.action, restored.pointer_flipped) == (RunFileAction.SAME_HASH, True)
    assert run.pointers_flipped == 1
