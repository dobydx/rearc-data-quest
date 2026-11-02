"""Typed job configuration, parsed from the string parameters Databricks passes to a task."""

import argparse
import importlib
import os
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from quest import __version__
from quest.runtime import dbutils

DEFAULT_BLS_BASE_URL = "https://download.bls.gov/pub/time.series/pr/"
DEFAULT_DATAUSA_URL = (
    "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
    "?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
)

CONTACT_EMAIL_ENV_VAR = "BLS_CONTACT_EMAIL"
CONTACT_EMAIL_SCOPE = "quest"
CONTACT_EMAIL_KEY = "bls_contact_email"


class ConfigError(ValueError):
    """Raised when job configuration is missing or invalid."""


def utc_now() -> datetime:
    """Return the current time as timezone-aware UTC. Every module takes its clock from here."""
    return datetime.now(UTC)


@dataclass(frozen=True)
class IngestConfig:
    """Settings for the ingest task."""

    catalog: str
    landing_root: Path
    bls_base_url: str
    datausa_url: str
    contact_email: str
    run_id: str
    job_id: str | None = None
    job_run_id: str | None = None
    trigger: str = "manual"
    # The deployed code version; the job sets the same value as the pipeline's quest.code_version.
    code_version: str = __version__
    force_pipeline: bool = False
    min_request_interval_s: float = 1.0
    connect_timeout_s: float = 10.0
    read_timeout_s: float = 60.0
    max_attempts: int = 5
    min_active_ratio: float = 0.5

    def __post_init__(self) -> None:
        """Reject values that would fail later in a less obvious way."""
        if not 0.0 <= self.min_active_ratio <= 1.0:
            raise ConfigError(f"min_active_ratio must be in [0, 1], got {self.min_active_ratio}")
        if self.max_attempts < 1:
            raise ConfigError(f"max_attempts must be at least 1, got {self.max_attempts}")
        if self.min_request_interval_s < 0:
            raise ConfigError("min_request_interval_s must not be negative")
        if self.connect_timeout_s <= 0 or self.read_timeout_s <= 0:
            raise ConfigError("timeouts must be positive")


@dataclass(frozen=True)
class ValidateConfig:
    """Settings for the validate task."""

    catalog: str
    run_id: str
    ingest_pointers_flipped: int = 0
    # Ingest's gate saw a new code version, so the pipeline rebuilt tables on purpose.
    ingest_code_changed: bool = False

    def __post_init__(self) -> None:
        """Reject a negative count of flipped pointers."""
        if self.ingest_pointers_flipped < 0:
            raise ConfigError(
                f"ingest_pointers_flipped must not be negative, got {self.ingest_pointers_flipped}"
            )


def _parse_bool(value: str) -> bool:
    """Parse the string booleans that Databricks job parameters arrive as."""
    lowered = value.strip().lower()
    if lowered in {"true", "1", "yes"}:
        return True
    if lowered in {"false", "0", "no"}:
        return False
    raise argparse.ArgumentTypeError(f"expected true or false, got {value!r}")


def parse_ingest_args(argv: Sequence[str]) -> IngestConfig:
    """Build an ``IngestConfig`` from command-line style job parameters."""
    parser = argparse.ArgumentParser(prog="quest-ingest")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--landing-root", required=True, type=Path)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--contact-email", default=None)
    parser.add_argument("--bls-base-url", default=DEFAULT_BLS_BASE_URL)
    parser.add_argument("--datausa-url", default=DEFAULT_DATAUSA_URL)
    # Audit only; a job passes {{job.id}}, {{job.run_id}} and {{job.trigger.type}}.
    parser.add_argument("--job-id", default=None)
    parser.add_argument("--job-run-id", default=None)
    parser.add_argument("--trigger", default=None)
    parser.add_argument("--code-version", default=None)
    # Optional overrides default to None so the dataclass stays the one place that owns defaults.
    parser.add_argument("--force-pipeline", type=_parse_bool, default=None)
    parser.add_argument("--min-active-ratio", type=float, default=None)
    parser.add_argument("--max-attempts", type=int, default=None)
    args = parser.parse_args(argv)

    overrides = {
        "trigger": args.trigger,
        "code_version": args.code_version,
        "force_pipeline": args.force_pipeline,
        "min_active_ratio": args.min_active_ratio,
        "max_attempts": args.max_attempts,
    }
    return IngestConfig(
        catalog=args.catalog,
        landing_root=args.landing_root,
        bls_base_url=args.bls_base_url,
        datausa_url=args.datausa_url,
        contact_email=resolve_contact_email(args.contact_email),
        run_id=args.run_id,
        job_id=args.job_id,
        job_run_id=args.job_run_id,
        **{name: value for name, value in overrides.items() if value is not None},
    )


def parse_validate_args(argv: Sequence[str]) -> ValidateConfig:
    """Build a ``ValidateConfig`` from command-line style job parameters."""
    parser = argparse.ArgumentParser(prog="quest-validate")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--ingest-pointers-flipped", type=int, default=0)
    parser.add_argument("--ingest-code-changed", type=_parse_bool, default=False)
    args = parser.parse_args(argv)
    return ValidateConfig(
        catalog=args.catalog,
        run_id=args.run_id,
        ingest_pointers_flipped=args.ingest_pointers_flipped,
        ingest_code_changed=args.ingest_code_changed,
    )


def _read_secret(scope: str, key: str) -> str | None:
    """Read a Databricks secret, or return None when not running on Databricks.

    Raises:
        ConfigError: If the SDK cannot read the secret, for example when it does not exist.
    """
    handle = dbutils()
    if handle is None:
        return None
    sdk_errors = importlib.import_module("databricks.sdk.errors")
    try:
        secret: str = handle.secrets.get(scope, key)
    except sdk_errors.DatabricksError as exc:
        raise ConfigError(f"cannot read secret {scope}/{key}: {exc}") from exc
    return secret


def resolve_contact_email(cli_value: str | None) -> str:
    """Return the BLS contact email from the CLI, the environment, then the Databricks secret.

    BLS asks automated clients to identify themselves, so the email goes in the User-Agent.

    Raises:
        ConfigError: If none of the three sources supplies a value.
    """
    # Short-circuit so the secret is only read when the cheaper sources are empty.
    email = (
        cli_value
        or os.environ.get(CONTACT_EMAIL_ENV_VAR)
        or _read_secret(CONTACT_EMAIL_SCOPE, CONTACT_EMAIL_KEY)
    )
    if not email:
        raise ConfigError(
            "contact email not found: pass --contact-email, set "
            f"{CONTACT_EMAIL_ENV_VAR}, or create secret {CONTACT_EMAIL_SCOPE}/{CONTACT_EMAIL_KEY}"
        )
    return email
