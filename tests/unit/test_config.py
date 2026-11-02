import sys
import types
from dataclasses import FrozenInstanceError
from datetime import timedelta
from pathlib import Path

import pytest

from quest import __version__, config
from quest.config import (
    DEFAULT_BLS_BASE_URL,
    DEFAULT_DATAUSA_URL,
    ConfigError,
    IngestConfig,
    ValidateConfig,
    parse_ingest_args,
    parse_validate_args,
    resolve_contact_email,
    utc_now,
)

INGEST_ARGV = [
    "--catalog",
    "workspace",
    "--landing-root",
    "/tmp/l",  # noqa: S108
    "--run-id",
    "r1",
    "--contact-email",
    "a@b.c",
]


def make_ingest_config(**overrides: object) -> IngestConfig:
    values: dict[str, object] = {
        "catalog": "workspace",
        "landing_root": Path("/tmp/l"),  # noqa: S108
        "bls_base_url": DEFAULT_BLS_BASE_URL,
        "datausa_url": DEFAULT_DATAUSA_URL,
        "contact_email": "a@b.c",
        "run_id": "r1",
    }
    values.update(overrides)
    return IngestConfig(**values)  # type: ignore[arg-type]


def test_parse_ingest_args_defaults() -> None:
    cfg = parse_ingest_args(INGEST_ARGV)

    assert cfg.catalog == "workspace"
    assert cfg.landing_root == Path("/tmp/l")  # noqa: S108
    assert cfg.bls_base_url == DEFAULT_BLS_BASE_URL
    assert cfg.datausa_url == DEFAULT_DATAUSA_URL
    assert cfg.contact_email == "a@b.c"
    assert cfg.force_pipeline is False
    assert cfg.max_attempts == 5


def test_parse_ingest_args_code_version_defaults_to_package_version() -> None:
    assert parse_ingest_args(INGEST_ARGV).code_version == __version__


def test_parse_ingest_args_code_version_from_job() -> None:
    assert parse_ingest_args([*INGEST_ARGV, "--code-version", "1.4.0"]).code_version == "1.4.0"


def test_parse_ingest_args_audit_fields_default_to_manual_run() -> None:
    cfg = parse_ingest_args(INGEST_ARGV)

    assert (cfg.job_id, cfg.job_run_id, cfg.trigger) == (None, None, "manual")


def test_parse_ingest_args_audit_fields_from_job() -> None:
    cfg = parse_ingest_args(
        [*INGEST_ARGV, "--job-id", "7", "--job-run-id", "42", "--trigger", "PERIODIC"]
    )

    assert (cfg.job_id, cfg.job_run_id, cfg.trigger) == ("7", "42", "PERIODIC")


@pytest.mark.parametrize(("raw", "expected"), [("true", True), ("False", False), ("1", True)])
def test_parse_ingest_args_force_pipeline_is_string_boolean(raw: str, expected: bool) -> None:
    cfg = parse_ingest_args([*INGEST_ARGV, "--force-pipeline", raw])

    assert cfg.force_pipeline is expected


def test_parse_ingest_args_rejects_unknown_boolean() -> None:
    with pytest.raises(SystemExit):
        parse_ingest_args([*INGEST_ARGV, "--force-pipeline", "maybe"])


def test_parse_ingest_args_numeric_overrides_arrive_as_strings() -> None:
    cfg = parse_ingest_args([*INGEST_ARGV, "--min-active-ratio", "0.25", "--max-attempts", "3"])

    assert cfg.min_active_ratio == 0.25
    assert cfg.max_attempts == 3


def test_config_rejects_bad_ratio() -> None:
    with pytest.raises(ConfigError, match="min_active_ratio"):
        make_ingest_config(min_active_ratio=1.5)


def test_config_rejects_zero_attempts() -> None:
    with pytest.raises(ConfigError, match="max_attempts"):
        make_ingest_config(max_attempts=0)


def test_ingest_config_is_frozen() -> None:
    cfg = make_ingest_config()

    with pytest.raises(FrozenInstanceError):
        cfg.run_id = "other"  # type: ignore[misc]


def test_parse_validate_args() -> None:
    cfg = parse_validate_args(
        ["--catalog", "workspace", "--run-id", "r1", "--ingest-pointers-flipped", "2"]
    )

    assert cfg == ValidateConfig(catalog="workspace", run_id="r1", ingest_pointers_flipped=2)


def test_parse_validate_args_reads_the_code_changed_flag() -> None:
    base = ["--catalog", "workspace", "--run-id", "r1"]

    assert parse_validate_args(base).ingest_code_changed is False
    assert parse_validate_args([*base, "--ingest-code-changed", "true"]).ingest_code_changed


def test_validate_config_rejects_negative_pointers_flipped() -> None:
    with pytest.raises(ConfigError, match="ingest_pointers_flipped"):
        ValidateConfig(catalog="workspace", run_id="r1", ingest_pointers_flipped=-1)


def test_utc_now_is_timezone_aware_utc() -> None:
    assert utc_now().utcoffset() == timedelta(0)


def test_contact_email_cli_value_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BLS_CONTACT_EMAIL", "env@b.c")

    assert resolve_contact_email("cli@b.c") == "cli@b.c"


def test_contact_email_falls_back_to_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BLS_CONTACT_EMAIL", "env@b.c")

    assert resolve_contact_email(None) == "env@b.c"


def test_contact_email_falls_back_to_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BLS_CONTACT_EMAIL", raising=False)
    monkeypatch.setattr(config, "_read_secret", lambda scope, key: "secret@b.c")

    assert resolve_contact_email(None) == "secret@b.c"


def test_contact_email_missing_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BLS_CONTACT_EMAIL", raising=False)
    monkeypatch.setattr(config, "_read_secret", lambda scope, key: None)

    with pytest.raises(ConfigError, match="contact email"):
        resolve_contact_email(None)


def test_read_secret_returns_none_outside_databricks() -> None:
    assert config._read_secret("quest", "bls_contact_email") is None


def _secret_must_not_be_read(scope: str, key: str) -> str | None:
    raise AssertionError(f"secret {scope}/{key} read although a higher-priority source was set")


def test_contact_email_cli_and_env_never_read_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "_read_secret", _secret_must_not_be_read)
    monkeypatch.setenv("BLS_CONTACT_EMAIL", "env@b.c")

    assert resolve_contact_email("cli@b.c") == "cli@b.c"
    assert resolve_contact_email(None) == "env@b.c"


class FakeDatabricksError(Exception):
    pass


def _install_fake_databricks_sdk(monkeypatch: pytest.MonkeyPatch, get: object) -> None:
    errors = types.ModuleType("databricks.sdk.errors")
    errors.DatabricksError = FakeDatabricksError  # type: ignore[attr-defined]
    runtime = types.ModuleType("databricks.sdk.runtime")
    runtime.dbutils = types.SimpleNamespace(secrets=types.SimpleNamespace(get=get))  # type: ignore[attr-defined]
    for name, module in {
        "databricks": types.ModuleType("databricks"),
        "databricks.sdk": types.ModuleType("databricks.sdk"),
        "databricks.sdk.errors": errors,
        "databricks.sdk.runtime": runtime,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)


def test_read_secret_converts_sdk_error_to_config_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def missing(scope: str, key: str) -> str:
        raise FakeDatabricksError("Secret does not exist")

    _install_fake_databricks_sdk(monkeypatch, missing)

    with pytest.raises(ConfigError, match="quest/bls_contact_email"):
        config._read_secret("quest", "bls_contact_email")


def test_read_secret_returns_value_on_databricks(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_databricks_sdk(monkeypatch, lambda scope, key: "s@b.c")

    assert config._read_secret("quest", "bls_contact_email") == "s@b.c"
