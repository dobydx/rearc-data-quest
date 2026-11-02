"""Content-addressed landing: ``<root>/<source>/<file_name>/sha256=<hex>/<file_name>``."""

import hashlib
from dataclasses import dataclass
from pathlib import Path


class LandingError(OSError):
    """Raised when a landed file does not read back at the expected size."""


@dataclass(frozen=True)
class LandedFile:
    """Where a blob landed, and whether this call wrote it."""

    path: Path
    sha256: str
    size: int
    written: bool


def sha256_hex(body: bytes) -> str:
    """Return the lowercase hex SHA-256 of ``body``."""
    return hashlib.sha256(body).hexdigest()


def landing_path(root: Path, source: str, file_name: str, sha256: str) -> Path:
    """Return the content-addressed path of one file version."""
    return root / source / file_name / f"sha256={sha256}" / file_name


def land(root: Path, source: str, file_name: str, body: bytes) -> LandedFile:
    """Write ``body`` to its content address unless an identical-size file is already there.

    The path is unique per content, so a plain write is safe to repeat; no temp file and
    rename, which Volumes do not make atomic anyway.

    Raises:
        LandingError: If the file on disk does not have ``len(body)`` bytes after the write.
    """
    sha256 = sha256_hex(body)
    path = landing_path(root, source, file_name, sha256)
    written = not path.exists() or path.stat().st_size != len(body)
    if written:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    size = path.stat().st_size
    if size != len(body):
        raise LandingError(f"{path} has {size} bytes after landing, expected {len(body)}")
    return LandedFile(path=path, sha256=sha256, size=size, written=written)


def count_data_lines(body: bytes) -> int:
    """Count non-blank lines minus the header, which is what bronze should hold."""
    non_blank = sum(1 for line in body.splitlines() if line.strip())
    return max(non_blank - 1, 0)
