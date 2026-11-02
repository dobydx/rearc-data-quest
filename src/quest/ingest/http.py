"""The one HTTP client: polite pacing, a contact User-Agent and an explicit retry policy."""

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import format_datetime, parsedate_to_datetime
from types import TracebackType
from typing import Self

import httpx
from tenacity import (
    RetryCallState,
    RetryError,
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from quest import __version__
from quest.config import utc_now
from quest.log import get_logger

BLS_POLICY_URL = "https://www.bls.gov/bls/pss.htm"
BACKOFF_MAX_S = 30.0
_BACKOFF = wait_exponential_jitter(initial=1, max=BACKOFF_MAX_S)


class HttpError(Exception):
    """Raised when a GET does not produce a 200 or 304."""


class ForbiddenError(HttpError):
    """Raised on 403, which BLS returns when a client breaks its access policy."""


class _RetryableError(Exception):
    """A transient failure (429, 5xx, connection error or timeout) worth another attempt."""

    def __init__(self, message: str, retry_after_s: float | None = None) -> None:
        super().__init__(message)
        self.retry_after_s = retry_after_s


@dataclass(frozen=True)
class HttpResponse:
    """A finished 200 or 304 response; header keys are lowercase."""

    status: int
    body: bytes
    headers: Mapping[str, str]


def user_agent(contact_email: str) -> str:
    """Return the User-Agent BLS asks automated clients to send."""
    return f"rearc-data-quest/{__version__} (contact: {contact_email})"


def format_http_date(dt: datetime) -> str:
    """Format a datetime as an RFC 7231 HTTP date in GMT."""
    return format_datetime(dt.astimezone(UTC), usegmt=True)


def parse_http_date(value: str) -> datetime:
    """Parse an RFC 7231 HTTP date into an aware UTC datetime.

    Raises:
        ValueError: If ``value`` is not an HTTP date.
    """
    parsed = parsedate_to_datetime(value)
    # "-0000" parses as naive; HTTP dates are always GMT.
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


class HttpClient:
    """Sequential GETs at least ``min_interval_s`` apart, retried with backoff and jitter."""

    def __init__(
        self,
        user_agent: str,
        connect_timeout_s: float,
        read_timeout_s: float,
        max_attempts: int,
        min_interval_s: float,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        """Open the underlying httpx client; ``sleep`` is injectable so tests never wait."""
        self._client = httpx.Client(
            headers={"User-Agent": user_agent},
            timeout=httpx.Timeout(read_timeout_s, connect=connect_timeout_s),
            follow_redirects=True,
        )
        self._max_attempts = max_attempts
        self._min_interval_s = min_interval_s
        self._sleep = sleep
        self._last_request_at: float | None = None
        self._log = get_logger(__name__)

    def __enter__(self) -> Self:
        """Return the client for use in a ``with`` block."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close the connection pool."""
        self._client.close()

    def get(self, url: str, if_modified_since: datetime | None = None) -> HttpResponse:
        """GET ``url`` and return the 200 or 304 response.

        Raises:
            ForbiddenError: On 403, never retried.
            HttpError: On any other non-retryable status, or once every attempt has failed.
        """
        headers = {}
        if if_modified_since is not None:
            headers["If-Modified-Since"] = format_http_date(if_modified_since)
        retrying = Retrying(
            stop=stop_after_attempt(self._max_attempts),
            wait=_retry_after_or_backoff,
            retry=retry_if_exception_type(_RetryableError),
            sleep=self._sleep,
            before_sleep=self._log_retry,
        )
        try:
            return retrying(self._attempt, url, headers)
        except RetryError as exc:
            last = exc.last_attempt.exception()
            message = f"GET {url} failed after {self._max_attempts} attempts: {last}"
            raise HttpError(message) from last

    def _attempt(self, url: str, headers: Mapping[str, str]) -> HttpResponse:
        self._pace()
        started = time.monotonic()
        try:
            response = self._client.get(url, headers=headers)
        except httpx.TransportError as exc:  # also covers every timeout
            self._log.warning("http_transport_error", url=url, error=repr(exc))
            raise _RetryableError(f"{type(exc).__name__}: {exc}") from exc
        status = response.status_code
        self._log.info(
            "http_response",
            url=url,
            status=status,
            elapsed_ms=round((time.monotonic() - started) * 1000),
        )
        if status in (200, 304):
            return HttpResponse(
                status=status,
                body=response.content,
                headers={key.lower(): value for key, value in response.headers.items()},
            )
        if status == 403:
            raise ForbiddenError(
                f"GET {url} returned 403; check the User-Agent against {BLS_POLICY_URL}"
            )
        if status == 429:
            retry_after = _retry_after_seconds(response.headers.get("Retry-After"))
            raise _RetryableError(f"GET {url} returned 429", retry_after_s=retry_after)
        if status >= 500:
            raise _RetryableError(f"GET {url} returned {status}")
        raise HttpError(f"GET {url} returned {status}")

    def _pace(self) -> None:
        """Sleep until ``min_interval_s`` has passed since the previous request."""
        if self._last_request_at is not None:
            remaining = self._min_interval_s - (time.monotonic() - self._last_request_at)
            if remaining > 0:
                self._sleep(remaining)
        self._last_request_at = time.monotonic()

    def _log_retry(self, retry_state: RetryCallState) -> None:
        self._log.warning(
            "http_retry_scheduled",
            attempt=retry_state.attempt_number,
            wait_s=round(retry_state.upcoming_sleep, 3),
            error=str(retry_state.outcome.exception()) if retry_state.outcome else None,
        )


def _retry_after_or_backoff(retry_state: RetryCallState) -> float:
    error = retry_state.outcome.exception() if retry_state.outcome else None
    if isinstance(error, _RetryableError) and error.retry_after_s is not None:
        # Capped: a server asking for an hour (or "inf") must not stall the task.
        return min(error.retry_after_s, BACKOFF_MAX_S)
    return _BACKOFF(retry_state)


def _retry_after_seconds(value: str | None) -> float | None:
    """Read Retry-After as seconds or as an HTTP date; None if absent or unreadable."""
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        return max(0.0, (parse_http_date(value) - utc_now()).total_seconds())
    except ValueError:
        return None
