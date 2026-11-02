"""Read the SELECT of each gold materialized view out of gold.sql, for tests and parity checks."""

import re
from pathlib import Path

# gold.sql layout: the query starts after a line that is exactly "AS" and ends at "...;".
_STATEMENT = re.compile(
    r"CREATE OR REFRESH MATERIALIZED VIEW quest_gold\.(?P<name>\w+)\b"
    r".*?^AS\n(?P<select>.*?);[ \t]*$",
    re.DOTALL | re.MULTILINE,
)


def load_gold_selects(path: Path) -> dict[str, str]:
    """Map each gold table name to its query text (after ``AS``, without the ``;``)."""
    return {
        match["name"]: match["select"].strip() for match in _STATEMENT.finditer(path.read_text())
    }
