import json
from datetime import UTC, datetime, timedelta

import pytest
from pyspark.sql import SparkSession

from quest.ingest.delta_ops_store import latest_real_update_state

# Frames and a temp view only, so every test here also runs on serverless.
pytestmark = [pytest.mark.spark, pytest.mark.connect_safe]

EVENT_LOG_DDL = (
    "event_type STRING, timestamp TIMESTAMP, origin STRUCT<update_id: STRING>, details STRING"
)
START = datetime(2026, 11, 3, 12, 0, tzinfo=UTC)

Event = tuple[str, datetime, dict[str, str], str]


def update(update_id: str, minute: int, states: list[str], validate_only: bool) -> list[Event]:
    """The create_update event and one update_progress event per state, a minute apart."""
    created = START + timedelta(minutes=minute)
    create = {"create_update": {"validate_only": validate_only, "cause": "API_CALL"}}
    events = [("create_update", created, {"update_id": update_id}, json.dumps(create))]
    for offset, state in enumerate(states, start=1):
        progress = {"update_progress": {"state": state}}
        at = created + timedelta(seconds=offset)
        events.append(("update_progress", at, {"update_id": update_id}, json.dumps(progress)))
    return events


def state_of(spark: SparkSession, *updates: list[Event]) -> str | None:
    rows = [event for events in updates for event in events]
    spark.createDataFrame(rows, EVENT_LOG_DDL).createOrReplaceTempView("event_log")
    return latest_real_update_state(spark, "event_log")


def test_dry_run_after_a_failed_update_does_not_hide_the_failure(spark: SparkSession) -> None:
    failed = update("u1", 0, ["RUNNING", "FAILED"], validate_only=False)
    dry_run = update("u2", 5, ["RUNNING", "COMPLETED"], validate_only=True)

    assert state_of(spark, failed, dry_run) == "FAILED"


def test_real_update_in_flight_is_its_current_state(spark: SparkSession) -> None:
    done = update("u1", 0, ["RUNNING", "COMPLETED"], validate_only=False)
    running = update("u2", 5, ["INITIALIZING", "RUNNING"], validate_only=False)

    assert state_of(spark, done, running) == "RUNNING"


def test_latest_real_update_wins_over_older_ones(spark: SparkSession) -> None:
    failed = update("u1", 0, ["RUNNING", "FAILED"], validate_only=False)
    done = update("u2", 5, ["RUNNING", "COMPLETED"], validate_only=False)
    dry_run = update("u3", 9, ["RUNNING", "FAILED"], validate_only=True)

    assert state_of(spark, failed, done, dry_run) == "COMPLETED"


def test_only_dry_runs_means_no_real_update(spark: SparkSession) -> None:
    dry_run = update("u1", 0, ["RUNNING", "COMPLETED"], validate_only=True)

    assert state_of(spark, dry_run) is None
