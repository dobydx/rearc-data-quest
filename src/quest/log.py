"""Structured JSON logging on stdout, which Databricks captures as the task's driver log."""

import sys

import structlog


def configure_logging(run_id: str) -> None:
    """Send JSON log lines to stdout and stamp every line with ``run_id``."""
    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(run_id=run_id)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        # PrintLoggerFactory resolves sys.stdout when each logger is built, not at import.
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a logger whose lines carry ``logger=name``.

    Call it after ``configure_logging``: the logger binds its configuration when created.
    """
    return structlog.stdlib.get_logger().bind(logger=name)
