import sys
import types

import pytest

from quest.runtime import dbutils


def test_dbutils_is_none_off_databricks() -> None:
    assert dbutils() is None


def test_dbutils_from_sdk_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    runtime = types.ModuleType("databricks.sdk.runtime")
    marker = object()
    runtime.dbutils = marker  # type: ignore[attr-defined]
    for name, module in {
        "databricks": types.ModuleType("databricks"),
        "databricks.sdk": types.ModuleType("databricks.sdk"),
        "databricks.sdk.runtime": runtime,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)

    assert dbutils() is marker
