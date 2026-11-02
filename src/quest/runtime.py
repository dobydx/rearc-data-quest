"""Access to Databricks runtime objects that only exist on a cluster or serverless compute."""

import importlib
from typing import Any


def dbutils() -> Any | None:
    """Return the Databricks ``dbutils`` handle, or None when not running on Databricks."""
    try:
        # Imported by name so local runs, where databricks-sdk is absent, skip it quietly.
        runtime = importlib.import_module("databricks.sdk.runtime")
    except ImportError:
        return None
    return runtime.dbutils
