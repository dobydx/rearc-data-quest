from collections.abc import Iterator
from datetime import UTC, datetime

import httpx
import pytest
import respx

from quest import __version__
from quest.ingest.http import (
    ForbiddenError,
    HttpClient,
    HttpError,
    format_http_date,
    parse_http_date,
    user_agent,
)

URL = "https://download.bls.gov/pub/time.series/pr/pr.series"
UA = "rearc-data-quest/test (contact: a@b.c)"


class RecordingSleep:
    def __init__(self) -> None:
        self.calls: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)


@pytest.fixture
def sleep() -> RecordingSleep:
    return RecordingSleep()


@pytest.fixture
def client(sleep: RecordingSleep) -> Iterator[HttpClient]:
    with HttpClient(
        user_agent=UA,
        connect_timeout_s=10.0,
        read_timeout_s=60.0,
        max_attempts=5,
        min_interval_s=1.0,
        sleep=sleep,
    ) as http:
        yield http


@respx.mock
def test_sends_user_agent_and_ims_header(client: HttpClient) -> None:
    route = respx.get(URL).mock(return_value=httpx.Response(200, content=b"x"))
    ims = datetime(2026, 9, 3, 12, 30, tzinfo=UTC)

    response = client.get(URL, if_modified_since=ims)

    request = route.calls.last.request
    assert request.headers["user-agent"] == UA
    assert request.headers["if-modified-since"] == "Thu, 03 Sep 2026 12:30:00 GMT"
    assert response.status == 200
    assert response.body == b"x"


@respx.mock
def test_no_ims_header_when_not_given(client: HttpClient) -> None:
    route = respx.get(URL).mock(return_value=httpx.Response(200))

    client.get(URL)

    assert "if-modified-since" not in route.calls.last.request.headers


@respx.mock
def test_response_header_keys_are_lowercase(client: HttpClient) -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, headers={"Last-Modified": "x"}))

    assert client.get(URL).headers["last-modified"] == "x"


@respx.mock
def test_304_returned_not_raised(client: HttpClient) -> None:
    respx.get(URL).mock(return_value=httpx.Response(304))

    assert client.get(URL, if_modified_since=datetime(2026, 1, 1, tzinfo=UTC)).status == 304


@respx.mock
def test_403_raises_forbidden_without_retry(client: HttpClient) -> None:
    route = respx.get(URL).mock(return_value=httpx.Response(403))

    with pytest.raises(ForbiddenError, match=r"https://www\.bls\.gov/bls/pss\.htm"):
        client.get(URL)

    assert route.call_count == 1


@respx.mock
def test_404_raises_without_retry(client: HttpClient) -> None:
    route = respx.get(URL).mock(return_value=httpx.Response(404))

    with pytest.raises(HttpError, match="404"):
        client.get(URL)

    assert route.call_count == 1


@respx.mock
def test_5xx_then_200_retries(client: HttpClient) -> None:
    route = respx.get(URL).mock(side_effect=[httpx.Response(503), httpx.Response(200)])

    assert client.get(URL).status == 200
    assert route.call_count == 2


@respx.mock
def test_429_honours_retry_after(client: HttpClient, sleep: RecordingSleep) -> None:
    respx.get(URL).mock(
        side_effect=[httpx.Response(429, headers={"Retry-After": "7"}), httpx.Response(200)]
    )

    assert client.get(URL).status == 200
    assert 7.0 in sleep.calls


@respx.mock
def test_gives_up_after_max_attempts(client: HttpClient) -> None:
    route = respx.get(URL).mock(return_value=httpx.Response(500))

    with pytest.raises(HttpError, match="5 attempts"):
        client.get(URL)

    assert route.call_count == 5


@respx.mock
def test_backoff_waits_stay_within_bounds(client: HttpClient, sleep: RecordingSleep) -> None:
    respx.get(URL).mock(return_value=httpx.Response(500))

    with pytest.raises(HttpError):
        client.get(URL)

    backoffs = [s for s in sleep.calls if s >= 1.0]
    assert len(backoffs) >= 4
    assert all(1.0 <= s <= 30.0 for s in backoffs)


@respx.mock
def test_timeout_is_retried(client: HttpClient) -> None:
    route = respx.get(URL).mock(side_effect=[httpx.ReadTimeout("slow"), httpx.Response(200)])

    assert client.get(URL).status == 200
    assert route.call_count == 2


@respx.mock
def test_connection_error_is_retried(client: HttpClient) -> None:
    route = respx.get(URL).mock(side_effect=[httpx.ConnectError("down"), httpx.Response(200)])

    assert client.get(URL).status == 200
    assert route.call_count == 2


@respx.mock
def test_enforces_min_interval(client: HttpClient, sleep: RecordingSleep) -> None:
    respx.get(URL).mock(return_value=httpx.Response(200))

    client.get(URL)
    client.get(URL)

    assert len(sleep.calls) == 1
    assert 0 < sleep.calls[0] <= 1.0


def test_http_date_roundtrip() -> None:
    parsed = parse_http_date("Thu, 03 Sep 2026 12:30:00 GMT")

    assert parsed == datetime(2026, 9, 3, 12, 30, tzinfo=UTC)
    assert format_http_date(parsed) == "Thu, 03 Sep 2026 12:30:00 GMT"


def test_user_agent_names_version_and_contact() -> None:
    assert user_agent("a@b.c") == f"rearc-data-quest/{__version__} (contact: a@b.c)"


@pytest.mark.parametrize("header", ["3600", "inf"])
@respx.mock
def test_429_retry_after_is_capped_at_the_backoff_max(
    client: HttpClient, sleep: RecordingSleep, header: str
) -> None:
    # A huge or infinite Retry-After must not stall the task (time.sleep(inf) raises).
    respx.get(URL).mock(
        side_effect=[httpx.Response(429, headers={"Retry-After": header}), httpx.Response(200)]
    )

    assert client.get(URL).status == 200
    assert max(sleep.calls) == 30.0
