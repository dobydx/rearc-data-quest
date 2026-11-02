import json
from collections.abc import Iterator

import pytest
import structlog

from quest.log import configure_logging, get_logger


@pytest.fixture(autouse=True)
def _reset_structlog() -> Iterator[None]:
    yield
    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()


def test_log_line_is_json_with_run_id(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("r1")
    get_logger("x").info("hello", files=3)

    record = json.loads(capsys.readouterr().out.strip())

    assert record["run_id"] == "r1"
    assert record["event"] == "hello"
    assert record["files"] == 3
    assert record["level"] == "info"
    assert record["logger"] == "x"
    assert record["timestamp"].endswith("Z")
