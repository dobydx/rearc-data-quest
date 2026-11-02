"""Download real BLS and DataUSA files once and write the test fixtures under tests/fixtures/.

One-off generator. Rerun only to refresh the fixtures. Needs BLS_CONTACT_EMAIL in the
environment because BLS rejects requests that carry no contact in the User-Agent.
"""

import os
import time
import urllib.request
from pathlib import Path

BLS_BASE_URL = "https://download.bls.gov/pub/time.series/pr/"
DATAUSA_URL = (
    "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
    "?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
)
FIXTURES_DIR = Path(__file__).resolve().parent.parent / "tests" / "fixtures"
SLICE_SERIES = {
    "PRS30006032",
    "PRS30006013",
    "PRS84006021",
    "PRS84006102",
    "PRS32006212",
    "PRS30006081",
}
# BLS asks for no more than one request per second.
REQUEST_GAP_SECONDS = 1.0
REQUEST_TIMEOUT_SECONDS = 60
HEAD_LINE_COUNT = 20
# Lookup files are small, so they are kept whole.
FULL_FILES = ["measure", "sector", "class", "duration", "seasonal", "period", "footnote"]
# File name -> series ids to keep. Current holds only one series to stay small.
SERIES_SLICES = {
    "pr.data.1.AllData": SLICE_SERIES,
    "pr.data.0.Current": {"PRS30006032"},
    "pr.series": SLICE_SERIES,
}
HEAD_FILES = ["pr.txt", "pr.contacts"]


def fetch(url: str, user_agent: str) -> bytes:
    """Download a URL and return the raw bytes, pausing afterwards to respect the rate limit."""
    request = urllib.request.Request(url, headers={"User-Agent": user_agent})  # noqa: S310
    with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:  # noqa: S310
        body: bytes = response.read()
    time.sleep(REQUEST_GAP_SECONDS)
    return body


def keep_series_lines(raw: bytes, series_ids: set[str]) -> bytes:
    """Keep the header line and every line whose first tab-separated field is a wanted series."""
    lines = raw.splitlines(keepends=True)
    kept = [lines[0]]
    kept += [line for line in lines[1:] if line.split(b"\t", 1)[0].strip().decode() in series_ids]
    return b"".join(kept)


def keep_head(raw: bytes, line_count: int) -> bytes:
    """Keep the first line_count lines exactly as downloaded."""
    return b"".join(raw.splitlines(keepends=True)[:line_count])


def write_fixture(path: Path, content: bytes) -> None:
    """Write bytes to a fixture path, creating parent folders."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    print(f"wrote {path.relative_to(FIXTURES_DIR)} ({len(content)} bytes)")


def main() -> None:
    """Build every fixture file."""
    email = os.environ["BLS_CONTACT_EMAIL"]
    user_agent = f"rearc-data-quest/fixtures (contact: {email})"
    bls_dir = FIXTURES_DIR / "bls"

    write_fixture(FIXTURES_DIR / "bls_listing.html", fetch(BLS_BASE_URL, user_agent))
    for name, series_ids in SERIES_SLICES.items():
        raw = fetch(BLS_BASE_URL + name, user_agent)
        write_fixture(bls_dir / name, keep_series_lines(raw, series_ids))
    for suffix in FULL_FILES:
        name = f"pr.{suffix}"
        write_fixture(bls_dir / name, fetch(BLS_BASE_URL + name, user_agent))
    for name in HEAD_FILES:
        raw = fetch(BLS_BASE_URL + name, user_agent)
        write_fixture(bls_dir / name, keep_head(raw, HEAD_LINE_COUNT))
    write_fixture(FIXTURES_DIR / "datausa_population.json", fetch(DATAUSA_URL, user_agent))


if __name__ == "__main__":
    main()
