from pathlib import Path

from quest.ingest.listing import parse_listing
from quest.ingest.models import RemoteFile

BASE_URL = "https://download.bls.gov/pub/time.series/pr/"


def _real_listing(fixtures_dir: Path) -> list[RemoteFile]:
    return parse_listing((fixtures_dir / "bls_listing.html").read_text(), BASE_URL)


def test_parses_real_listing_12_files(fixtures_dir: Path) -> None:
    files = {f.name: f for f in _real_listing(fixtures_dir)}

    assert len(files) == 12
    alldata = files["pr.data.1.AllData"]
    assert alldata.size == 3239525
    assert alldata.listing_ts_raw == "9/3/2026 7:30 AM"
    assert files["pr.period"].listing_ts_raw == "1/7/1994 2:53 PM"


def test_parser_ignores_parent_and_dir_entries() -> None:
    html = (
        '<pre><A HREF="/pub/time.series/">[To Parent Directory]</A><br><br>'
        '11/18/2011  3:05 PM           79 <A HREF="/pub/time.series/pr/pr.seasonal">'
        "pr.seasonal</A><br>"
        ' 9/13/2022  3:52 PM        &lt;dir&gt; <A HREF="/pub/time.series/pr/old/">old</A><br>'
        '  9/3/2026  7:30 AM          102 <A HREF="/pub/time.series/pr/pr.class">'
        "pr.class</A><br></pre>"
    )

    files = parse_listing(html, BASE_URL)

    assert files == [
        RemoteFile("pr.seasonal", f"{BASE_URL}pr.seasonal", 79, "11/18/2011 3:05 PM"),
        RemoteFile("pr.class", f"{BASE_URL}pr.class", 102, "9/3/2026 7:30 AM"),
    ]


def test_parser_ignores_links_outside_base_path() -> None:
    html = '<pre>  9/3/2026  7:30 AM   10 <A HREF="/pub/time.series/ce/ce.series">x</A><br></pre>'

    assert parse_listing(html, BASE_URL) == []


def test_urls_are_absolute(fixtures_dir: Path) -> None:
    for remote in _real_listing(fixtures_dir):
        assert remote.url == f"{BASE_URL}{remote.name}"
