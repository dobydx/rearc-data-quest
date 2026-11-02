"""Validate entry point: capture table versions, run every check, write the results."""

import sys
from collections.abc import Sequence

from quest.config import parse_validate_args
from quest.ingest.delta_ops_store import DeltaOpsStore
from quest.log import configure_logging, get_logger
from quest.spark import get_spark
from quest.validate.checks import (
    ValidateContext,
    capture_table_versions,
    dq_check_row,
    run_checks,
)


class ValidationFailedError(Exception):
    """Raised after the results are written when any check failed or errored."""


def main(argv: Sequence[str] | None = None) -> int:
    """Run the validate task.

    Raises:
        ValidationFailedError: If any check has status ``fail`` or ``error``; raising (not a
            return code) is what fails a notebook task as well as a wheel task.
    """
    cfg = parse_validate_args(sys.argv[1:] if argv is None else argv)
    configure_logging(cfg.run_id)
    log = get_logger(__name__)
    spark = get_spark()
    store = DeltaOpsStore(spark, cfg.catalog)
    store.ensure_tables()
    ctx = ValidateContext(
        spark, cfg.run_id, cfg.ingest_pointers_flipped, code_changed=cfg.ingest_code_changed
    )
    # Versions first: NO_CHANGE_ON_RERUN reads this run's rows.
    store.merge("table_versions", capture_table_versions(ctx))
    results = run_checks(ctx)
    checked_at = ctx.now()
    store.merge("dq_checks", [dq_check_row(r, cfg.run_id, checked_at) for r in results])
    for result in results:
        log.info("check", **{k: v for k, v in vars(result).items()})
    failed = [f"{r.code}/{r.table_name}" for r in results if r.status in ("fail", "error")]
    if failed:
        raise ValidationFailedError("failed checks: " + ", ".join(failed))
    return 0
