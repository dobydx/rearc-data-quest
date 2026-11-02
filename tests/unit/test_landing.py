import hashlib
from pathlib import Path

from quest.ingest.landing import count_data_lines, land, landing_path, sha256_hex


def test_sha256_hex_matches_hashlib() -> None:
    assert sha256_hex(b"abc") == hashlib.sha256(b"abc").hexdigest()


def test_landing_path_layout(tmp_path: Path) -> None:
    h = "f" * 64

    path = landing_path(tmp_path, "bls_pr", "pr.series", h)

    assert path == tmp_path / "bls_pr" / "pr.series" / f"sha256={h}" / "pr.series"


def test_land_writes_body_at_content_address(tmp_path: Path) -> None:
    landed = land(tmp_path, "bls_pr", "pr.series", b"body")

    assert landed.written
    assert landed.sha256 == sha256_hex(b"body")
    assert landed.size == 4
    assert landed.path == landing_path(tmp_path, "bls_pr", "pr.series", landed.sha256)
    assert landed.path.read_bytes() == b"body"


def test_land_is_idempotent(tmp_path: Path) -> None:
    first = land(tmp_path, "bls_pr", "pr.series", b"body")
    mtime = first.path.stat().st_mtime_ns

    second = land(tmp_path, "bls_pr", "pr.series", b"body")

    assert not second.written
    assert second.path == first.path
    assert second.path.stat().st_mtime_ns == mtime


def test_land_rewrites_truncated_file(tmp_path: Path) -> None:
    first = land(tmp_path, "bls_pr", "pr.series", b"body")
    first.path.write_bytes(b"bo")

    again = land(tmp_path, "bls_pr", "pr.series", b"body")

    assert again.written
    assert again.path.read_bytes() == b"body"


def test_count_data_lines_ignores_blank_lines(fixtures_dir: Path) -> None:
    assert count_data_lines((fixtures_dir / "bls" / "pr.seasonal").read_bytes()) == 2


def test_count_data_lines_alldata_slice(fixtures_dir: Path) -> None:
    assert count_data_lines((fixtures_dir / "bls" / "pr.data.1.AllData").read_bytes()) == 1409


def test_count_data_lines_empty_body() -> None:
    assert count_data_lines(b"") == 0
