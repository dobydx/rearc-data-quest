from pathlib import Path


def test_alldata_slice_has_1409_rows(fixtures_dir: Path) -> None:
    lines = (fixtures_dir / "bls" / "pr.data.1.AllData").read_bytes().splitlines()
    assert len(lines) - 1 == 1409


def test_fixture_keeps_crlf_and_padding(fixtures_dir: Path) -> None:
    lines = (fixtures_dir / "bls" / "pr.data.1.AllData").read_bytes().splitlines(keepends=True)
    first_data_line = lines[1]
    assert first_data_line.startswith(b"PRS30006013      \t")
    assert first_data_line.endswith(b"\r\n")


def test_seasonal_has_trailing_blank_line(fixtures_dir: Path) -> None:
    assert (fixtures_dir / "bls" / "pr.seasonal").read_bytes().endswith(b"\r\n\r\n")
