"""Parse the IIS-style BLS directory listing into ``RemoteFile`` rows."""

import re
from html.parser import HTMLParser
from urllib.parse import unquote, urljoin, urlsplit

from quest.ingest.models import RemoteFile

# The text before each link: "M/D/YYYY  h:mm AM  <size> ". Directory rows say "<dir>" instead.
_ROW_PREFIX = re.compile(r"^\s*(?P<ts>\S+\s+\S+\s+[AP]M)\s+(?P<size>\d+)\s*$")


class _ListingParser(HTMLParser):
    """Pair each ``<a href>`` with the text that precedes it on its row."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[tuple[str, str]] = []
        self._pending_text: list[str] = []
        self._in_anchor = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "a":
            href = dict(attrs).get("href")
            if href:
                self.links.append((href, "".join(self._pending_text)))
            self._in_anchor = True
        self._pending_text = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "a":
            self._in_anchor = False

    def handle_data(self, data: str) -> None:
        if not self._in_anchor:
            self._pending_text.append(data)


def parse_listing(html: str, base_url: str) -> list[RemoteFile]:
    """Return the files listed directly under ``base_url``, in listing order."""
    parser = _ListingParser()
    parser.feed(html)
    parser.close()
    base_path = urlsplit(base_url).path
    files = []
    for href, prefix in parser.links:
        url = urljoin(base_url, href)
        name = urlsplit(url).path.removeprefix(base_path)
        row = _ROW_PREFIX.match(prefix)
        if row is None or not name or "/" in name or not url.startswith(base_url):
            continue
        files.append(
            RemoteFile(
                name=unquote(name),
                url=url,
                size=int(row["size"]),
                listing_ts_raw=" ".join(row["ts"].split()),
            )
        )
    return files
