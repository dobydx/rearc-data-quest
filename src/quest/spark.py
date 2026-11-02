"""The Spark session for job tasks, with the settings pinned everywhere the code runs."""

from collections.abc import Mapping

from pyspark.sql import SparkSession

SPARK_CONF: Mapping[str, str] = {
    # ANSI makes a bad cast fail instead of returning NULL; UTC makes timestamps comparable.
    "spark.sql.ansi.enabled": "true",
    "spark.sql.session.timeZone": "UTC",
}


def pin_conf(session: SparkSession) -> SparkSession:
    """Set ``SPARK_CONF`` on a live session and return it.

    Set on the session, not the builder: on Databricks the session already exists and
    ``getOrCreate`` ignores builder settings.
    """
    for key, value in SPARK_CONF.items():
        session.conf.set(key, value)
    return session


def get_spark() -> SparkSession:
    """Return the active session with ``SPARK_CONF`` applied."""
    return pin_conf(SparkSession.builder.getOrCreate())
