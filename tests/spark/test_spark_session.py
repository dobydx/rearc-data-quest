from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from quest.spark import SPARK_CONF, get_spark, pin_conf

pytestmark = pytest.mark.spark


@pytest.fixture
def unpinned(spark: SparkSession) -> Iterator[SparkSession]:
    spark.conf.set("spark.sql.ansi.enabled", "false")
    spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")
    try:
        yield spark
    finally:
        # The session is shared by every test; a failure here must not leave it unpinned.
        pin_conf(spark)


def _assert_pinned(session: SparkSession) -> None:
    assert session.conf.get("spark.sql.ansi.enabled") == "true"
    assert session.conf.get("spark.sql.session.timeZone") == "UTC"


def test_spark_conf_is_ansi_and_utc() -> None:
    assert dict(SPARK_CONF) == {
        "spark.sql.ansi.enabled": "true",
        "spark.sql.session.timeZone": "UTC",
    }


@pytest.mark.connect_safe
def test_pin_conf_sets_ansi_and_utc_on_a_live_session(unpinned: SparkSession) -> None:
    _assert_pinned(pin_conf(unpinned))


# Local only: get_spark() calls SparkSession.builder.getOrCreate(), which under
# databricks-connect is not the serverless session the spark fixture opened.
def test_get_spark_pins_ansi_and_utc(unpinned: SparkSession) -> None:
    _assert_pinned(get_spark())
