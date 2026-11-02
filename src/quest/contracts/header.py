"""Header contract check: normalize the source header and compare it to the contract."""

import io
from collections.abc import Sequence
from dataclasses import dataclass

from quest.contracts.models import FileContract


@dataclass(frozen=True)
class HeaderCheck:
    """Result of comparing an observed header with a contract."""

    ok: bool
    observed: tuple[str, ...]
    diff: str | None


def check_columns(contract: FileContract, tokens: Sequence[str]) -> HeaderCheck:
    """Compare header tokens, after strip and lowercase, to the contract's column aliases."""
    observed = tuple(token.strip().lower() for token in tokens)
    problems = [
        f"column {position} {column.name!r}: expected one of {list(column.source_aliases)}, "
        f"got {token!r}"
        for position, (column, token) in enumerate(zip(contract.columns, observed, strict=False), 1)
        if token not in column.source_aliases
    ]
    if len(observed) != len(contract.columns):
        problems.insert(0, f"expected {len(contract.columns)} columns, got {len(observed)}")
    return HeaderCheck(ok=not problems, observed=observed, diff="; ".join(problems) or None)


def check_header(contract: FileContract, body: bytes) -> HeaderCheck:
    """Check the first non-blank line of a tab-separated file against the contract."""
    return check_columns(contract, _first_non_blank_line(body).split("\t"))


def _first_non_blank_line(body: bytes) -> str:
    # BytesIO iterates lazily, so a 77k-line file is not split in full.
    for line in io.BytesIO(body):
        if line.strip():
            return line.decode("ascii", errors="replace")
    return ""
