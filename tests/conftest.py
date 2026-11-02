from collections.abc import Iterator
from pathlib import Path

import pytest
import structlog


@pytest.fixture
def fixtures_dir() -> Path:
    return Path(__file__).parent / "fixtures"


@pytest.fixture(autouse=True)
def _reset_structlog() -> Iterator[None]:
    # configure_logging binds the current sys.stdout, which capsys closes after each test.
    yield
    structlog.reset_defaults()
