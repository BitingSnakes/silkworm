from urllib.parse import parse_qsl, urlsplit
from typing import Any

import pytest

from silkworm.http import HttpClient
from silkworm.engine import Engine
from silkworm.middlewares import DelayMiddleware, RetryMiddleware
from silkworm.request import Request
from silkworm.response import HTMLResponse, Response
from silkworm.spiders import Spider


class _StubResponse:
    def __init__(
        self, *, status: Any = 200, headers: Any = None, body: bytes | str = b""
    ) -> None:
        self.status = status
        self.headers = headers or {}
        self._body = body

    async def read(self) -> bytes:
        if isinstance(self._body, bytes):
            return self._body
        return str(self._body).encode("utf-8")

    async def text(self) -> str:
        if isinstance(self._body, bytes):
            return self._body.decode("utf-8", errors="replace")
        return str(self._body)


class _RecordingClient:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, str, dict[str, Any]]] = []

    async def request(self, method: Any, url: str, **kwargs: Any) -> _StubResponse:
        self.calls.append((method, url, kwargs))
        return _StubResponse()


def test_request_replace_creates_new_request_with_updates():
    original = Request(
        url="https://example.com",
        method="GET",
        headers={"X-Token": "abc"},
        meta={"foo": 1},
    )

    updated = original.replace(method="POST", meta={"foo": 2})

    assert updated is not original
    assert updated.method == "POST"
    assert updated.meta["foo"] == 2
    assert original.method == "GET"
    assert original.meta["foo"] == 1


def test_response_follow_inherits_callback_and_joins_url():
    def callback(resp: Response) -> None:
        return None

    req = Request(url="http://example.com/dir/page", callback=callback)
    resp = Response(url=req.url, status=200, headers={}, body=b"", request=req)

    next_req = resp.follow("next")

    assert next_req.url == "http://example.com/dir/next"
    assert next_req.callback is callback


def test_htmlresponse_follow_works_with_slots():
    def callback(resp: Response) -> None:
        return None

    req = Request(url="http://example.com/dir/page", callback=callback)
    resp = HTMLResponse(url=req.url, status=200, headers={}, body=b"", request=req)

    next_req = resp.follow("next")

    assert next_req.url == "http://example.com/dir/next"
    assert next_req.callback is callback


def test_response_close_releases_payload():
    req = Request(url="http://example.com")
    resp = Response(
        url=req.url,
        status=200,
        headers={"Content-Type": "text/html"},
        body=b"abc",
        request=req,
    )

    resp.close()
    resp.close()  # idempotent

    assert resp.body == b""
    assert resp.headers == {}


def test_httpclient_build_url_merges_params_with_existing_query():
    client = HttpClient()
    req = Request(
        url="https://example.com/search?q=foo&lang=en",
        params={"page": 2, "q": "bar"},
    )

    built = client._build_url(req)
    parsed = dict(parse_qsl(urlsplit(built).query))

    assert parsed == {"q": "bar", "lang": "en", "page": "2"}


def test_httpclient_normalize_headers_handles_multiple_shapes():
    client = HttpClient()
    raw_headers = [
        b"Content-Type: text/html; charset=utf-8",
        ("X-Test", " value "),
        "X-RateLimit: 10",
        "InvalidHeaderWithoutColon",
    ]

    normalized = client._normalize_headers(raw_headers)

    assert normalized["content-type"] == "text/html; charset=utf-8"
    assert normalized["x-test"] == "value"
    assert normalized["x-ratelimit"] == "10"


async def test_httpclient_follows_redirects():
    class RedirectClient(_RecordingClient):
        async def request(self, method: Any, url: str, **kwargs: Any) -> _StubResponse:  # type: ignore[override]
            self.calls.append((method, url, kwargs))
            if url.startswith("http://example.com/start"):
                return _StubResponse(
                    status=302,
                    headers={"Location": "/next", "Content-Type": "text/plain"},
                )
            return _StubResponse(
                status=200, headers={"Content-Type": "text/plain"}, body=b"ok"
            )

    client = HttpClient()
    client._client = RedirectClient()  # type: ignore[assignment]

    resp = await client.fetch(
        Request(url="http://example.com/start", params={"foo": "1"})
    )

    assert resp.status == 200
    assert resp.url == "http://example.com/next"
    assert resp.body == b"ok"
    assert resp.request.url == "http://example.com/next"
    assert client._client.calls[0][1] == "http://example.com/start?foo=1"
    assert client._client.calls[1][1] == "http://example.com/next"


async def test_httpclient_detects_redirect_loops():
    from silkworm.exceptions import HttpError

    class LoopingClient(_RecordingClient):
        async def request(self, method: Any, url: str, **kwargs: Any) -> _StubResponse:  # type: ignore[override]
            self.calls.append((method, url, kwargs))
            return _StubResponse(
                status=302, headers={"Location": "/loop", "Content-Type": "text/plain"}
            )

    client = HttpClient(max_redirects=2)
    client._client = LoopingClient()  # type: ignore[assignment]

    with pytest.raises(HttpError):
        await client.fetch(Request(url="http://example.com/loop"))


async def test_httpclient_handles_unhashable_status_codes():
    class UnhashableStatus:
        __hash__ = None

        def __int__(self) -> int:
            return 302

    class RedirectClient(_RecordingClient):
        async def request(  # type: ignore[override]
            self, method: Any, url: str, **kwargs: Any
        ) -> _StubResponse:
            self.calls.append((method, url, kwargs))
            if len(self.calls) == 1:
                return _StubResponse(
                    status=UnhashableStatus(),
                    headers={"Location": "/next", "Content-Type": "text/plain"},
                )
            return _StubResponse(
                status=200, headers={"Content-Type": "text/plain"}, body=b"done"
            )

    client = HttpClient()
    redirect_client = RedirectClient()
    client._client = redirect_client  # type: ignore[assignment]

    resp = await client.fetch(Request(url="http://example.com/start"))

    assert resp.status == 200
    assert resp.url == "http://example.com/next"
    assert redirect_client.calls[0][1] == "http://example.com/start"
    assert redirect_client.calls[1][1] == "http://example.com/next"


async def test_httpclient_sets_keep_alive():
    client = HttpClient(keep_alive=True)
    recording = _RecordingClient()
    client._client = recording  # type: ignore[assignment]

    await client.fetch(Request(url="http://example.com/keep-alive"))

    assert recording.calls
    kwargs = recording.calls[0][2]
    assert kwargs.get("keep_alive") is True
    assert kwargs["headers"].get("Connection") == "keep-alive"


async def test_httpclient_keep_alive_when_kwarg_not_supported():
    class StrictClient:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        async def request(
            self,
            method: Any,
            url: str,
            *,
            headers: dict[str, str],
            data: Any = None,
            json: Any = None,
            proxy: Any = None,
            timeout: Any = None,
        ) -> _StubResponse:
            self.calls.append(
                {
                    "method": method,
                    "url": url,
                    "headers": headers,
                    "data": data,
                    "json": json,
                    "proxy": proxy,
                    "timeout": timeout,
                }
            )
            return _StubResponse(headers={"Content-Type": "text/plain"})

    client = HttpClient(keep_alive=True)
    strict = StrictClient()
    client._client = strict  # type: ignore[assignment]

    resp = await client.fetch(Request(url="http://example.com/no-kw"))

    assert resp.status == 200
    assert strict.calls
    first_call = strict.calls[0]
    assert first_call["headers"].get("Connection") == "keep-alive"


async def test_retry_middleware_returns_retry_request(monkeypatch: pytest.MonkeyPatch):
    sleep_calls: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    monkeypatch.setattr("silkworm.middlewares.asyncio.sleep", fake_sleep)

    middleware = RetryMiddleware(max_times=2, backoff_base=0.1)
    request = Request(url="http://example.com")
    response = Response(
        url=request.url, status=500, headers={}, body=b"", request=request
    )

    result = await middleware.process_response(response, Spider())

    assert isinstance(result, Request)
    assert result is not request
    assert result.meta["retry_times"] == 1
    assert result.dont_filter is True
    assert sleep_calls == [0.1]


async def test_retry_middleware_sleep_codes_extend_retry(
    monkeypatch: pytest.MonkeyPatch,
):
    sleep_calls: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    monkeypatch.setattr("silkworm.middlewares.asyncio.sleep", fake_sleep)

    middleware = RetryMiddleware(max_times=1, sleep_http_codes=[403], backoff_base=0.2)
    request = Request(url="http://example.com")
    response = Response(
        url=request.url, status=403, headers={}, body=b"", request=request
    )

    result = await middleware.process_response(response, Spider())

    assert isinstance(result, Request)
    assert result.meta["retry_times"] == 1
    assert sleep_calls == [0.2]


async def test_retry_middleware_retry_without_sleep(monkeypatch: pytest.MonkeyPatch):
    sleep_calls: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    monkeypatch.setattr("silkworm.middlewares.asyncio.sleep", fake_sleep)

    middleware = RetryMiddleware(
        max_times=1, retry_http_codes=[500], sleep_http_codes=[]
    )
    request = Request(url="http://example.com")
    response = Response(
        url=request.url, status=500, headers={}, body=b"", request=request
    )

    result = await middleware.process_response(response, Spider())

    assert isinstance(result, Request)
    assert result.meta["retry_times"] == 1
    assert sleep_calls == []


async def test_engine_closes_responses(monkeypatch: pytest.MonkeyPatch):
    class DummySpider(Spider):
        name = "closer"

        async def start_requests(self):
            yield Request(url="http://example.com", callback=self.parse)

        async def parse(self, response: Response):
            return None

    class ClosableResponse(Response):
        __slots__ = ("closed",)

        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            self.closed = False

        def close(self) -> None:
            self.closed = True

    spider = DummySpider()
    engine = Engine(spider, concurrency=1)
    seen: list[ClosableResponse] = []

    async def fake_fetch(req: Request) -> Response:
        resp = ClosableResponse(
            url=req.url, status=200, headers={}, body=b"", request=req
        )
        seen.append(resp)
        return resp

    engine.http.fetch = fake_fetch  # type: ignore[assignment]

    await engine.run()

    assert seen and all(r.closed for r in seen)


async def test_retry_middleware_stops_after_max_times(monkeypatch: pytest.MonkeyPatch):
    async def fake_sleep(_: float) -> None:
        return None

    monkeypatch.setattr("silkworm.middlewares.asyncio.sleep", fake_sleep)
    middleware = RetryMiddleware(max_times=1)
    request = Request(url="http://example.com", meta={"retry_times": 1})
    response = Response(
        url=request.url, status=500, headers={}, body=b"", request=request
    )

    result = await middleware.process_response(response, Spider())

    assert result is response


async def test_engine_retries_requests_even_if_url_seen(
    monkeypatch: pytest.MonkeyPatch,
):
    async def fake_sleep(_: float) -> None:
        return None

    monkeypatch.setattr("silkworm.middlewares.asyncio.sleep", fake_sleep)

    class DummySpider(Spider):
        name = "retryer"

        async def start_requests(self):
            yield Request(url="http://example.com", callback=self.parse)

        async def parse(self, response: Response):
            return None

    statuses = [429, 200]
    seen_requests: list[tuple[int, int]] = []

    engine = Engine(
        DummySpider(),
        concurrency=1,
        response_middlewares=[RetryMiddleware(max_times=1, backoff_base=0.0)],
    )

    async def fake_fetch(req: Request) -> Response:
        status = statuses.pop(0)
        retry_raw = req.meta.get("retry_times", 0)
        retry_times = int(retry_raw) if isinstance(retry_raw, (int, float, str)) else 0
        seen_requests.append((status, retry_times))
        return Response(url=req.url, status=status, headers={}, body=b"", request=req)

    engine.http.fetch = fake_fetch  # type: ignore[assignment]

    await engine.run()

    assert seen_requests == [(429, 0), (200, 1)]


async def test_delay_middleware_fixed_delay(monkeypatch: pytest.MonkeyPatch):
    sleep_calls: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    monkeypatch.setattr("silkworm.middlewares.asyncio.sleep", fake_sleep)

    middleware = DelayMiddleware(delay=1.5)
    request = Request(url="http://example.com")

    result = await middleware.process_request(request, Spider())

    assert result is request
    assert sleep_calls == [1.5]


async def test_delay_middleware_random_delay(monkeypatch: pytest.MonkeyPatch):
    sleep_calls: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    monkeypatch.setattr("silkworm.middlewares.asyncio.sleep", fake_sleep)

    middleware = DelayMiddleware(min_delay=0.5, max_delay=2.0)
    request = Request(url="http://example.com")

    result = await middleware.process_request(request, Spider())

    assert result is request
    assert len(sleep_calls) == 1
    assert 0.5 <= sleep_calls[0] <= 2.0


async def test_delay_middleware_custom_function(monkeypatch: pytest.MonkeyPatch):
    sleep_calls: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    monkeypatch.setattr("silkworm.middlewares.asyncio.sleep", fake_sleep)

    def custom_delay(request: Request, spider: Spider) -> float:
        return 3.0 if "slow" in request.url else 0.5

    middleware = DelayMiddleware(delay_func=custom_delay)

    slow_request = Request(url="http://slow.example.com")
    result1 = await middleware.process_request(slow_request, Spider())
    assert result1 is slow_request
    assert sleep_calls[-1] == 3.0

    fast_request = Request(url="http://fast.example.com")
    result2 = await middleware.process_request(fast_request, Spider())
    assert result2 is fast_request
    assert sleep_calls[-1] == 0.5


def test_delay_middleware_validation_errors():
    # Must provide at least one configuration
    with pytest.raises(ValueError, match="Must provide one of"):
        DelayMiddleware()

    # Cannot mix delay strategies
    with pytest.raises(ValueError, match="Cannot use both"):
        DelayMiddleware(delay=1.0, min_delay=0.5)

    with pytest.raises(ValueError, match="cannot be used with"):
        DelayMiddleware(delay=1.0, delay_func=lambda r, s: 1.0)

    # min_delay and max_delay must both be provided
    with pytest.raises(ValueError, match="Both min_delay and max_delay"):
        DelayMiddleware(min_delay=0.5)

    with pytest.raises(ValueError, match="Both min_delay and max_delay"):
        DelayMiddleware(max_delay=2.0)

    # Negative values not allowed
    with pytest.raises(ValueError, match="must be non-negative"):
        DelayMiddleware(delay=-1.0)

    with pytest.raises(ValueError, match="must be non-negative"):
        DelayMiddleware(min_delay=-0.5, max_delay=2.0)

    with pytest.raises(ValueError, match="must be non-negative"):
        DelayMiddleware(min_delay=0.5, max_delay=-2.0)

    # min_delay must be <= max_delay
    with pytest.raises(ValueError, match="must be less than or equal to"):
        DelayMiddleware(min_delay=2.0, max_delay=0.5)


async def test_delay_middleware_zero_delay(monkeypatch: pytest.MonkeyPatch):
    sleep_calls: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    monkeypatch.setattr("silkworm.middlewares.asyncio.sleep", fake_sleep)

    middleware = DelayMiddleware(delay=0.0)
    request = Request(url="http://example.com")

    result = await middleware.process_request(request, Spider())

    assert result is request
    # Zero delay should not call sleep
    assert sleep_calls == []


# ProxyMiddleware tests
async def test_proxy_middleware_round_robin_selection():
    from silkworm.middlewares import ProxyMiddleware

    proxies = ["http://proxy1:8080", "http://proxy2:8080", "http://proxy3:8080"]
    middleware = ProxyMiddleware(proxies=proxies)

    requests = [Request(url=f"http://example.com/{i}") for i in range(5)]
    results = [await middleware.process_request(req, Spider()) for req in requests]

    # Should cycle through proxies in order
    assert results[0].meta["proxy"] == "http://proxy1:8080"
    assert results[1].meta["proxy"] == "http://proxy2:8080"
    assert results[2].meta["proxy"] == "http://proxy3:8080"
    assert results[3].meta["proxy"] == "http://proxy1:8080"  # cycle back
    assert results[4].meta["proxy"] == "http://proxy2:8080"


async def test_proxy_middleware_random_selection():
    from silkworm.middlewares import ProxyMiddleware

    proxies = ["http://proxy1:8080", "http://proxy2:8080", "http://proxy3:8080"]
    middleware = ProxyMiddleware(proxies=proxies, random_selection=True)

    # Generate multiple requests to test random selection
    requests = [Request(url=f"http://example.com/{i}") for i in range(30)]
    results = [await middleware.process_request(req, Spider()) for req in requests]

    # All assigned proxies should be from our list
    assigned_proxies = [r.meta["proxy"] for r in results]
    assert all(p in proxies for p in assigned_proxies)

    # With random selection, we should see some variation
    # (statistically unlikely to get all the same proxy with 30 requests and 3 proxies)
    unique_proxies = set(assigned_proxies)
    assert len(unique_proxies) > 1


async def test_proxy_middleware_from_file(tmp_path):
    from silkworm.middlewares import ProxyMiddleware

    # Create a temporary proxy file
    proxy_file = tmp_path / "proxies.txt"
    proxy_file.write_text(
        "http://proxy1:8080\nhttp://proxy2:8080\n\nhttp://proxy3:8080\n"
    )

    middleware = ProxyMiddleware(proxy_file=proxy_file)

    assert len(middleware.proxies) == 3
    assert "http://proxy1:8080" in middleware.proxies
    assert "http://proxy2:8080" in middleware.proxies
    assert "http://proxy3:8080" in middleware.proxies

    # Test that it works
    request = Request(url="http://example.com")
    result = await middleware.process_request(request, Spider())
    assert result.meta["proxy"] in middleware.proxies


async def test_proxy_middleware_from_file_with_random_selection(tmp_path):
    from silkworm.middlewares import ProxyMiddleware

    # Create a temporary proxy file
    proxy_file = tmp_path / "proxies.txt"
    proxy_file.write_text(
        "http://proxy1:8080\nhttp://proxy2:8080\nhttp://proxy3:8080\n"
    )

    middleware = ProxyMiddleware(proxy_file=proxy_file, random_selection=True)

    # Generate multiple requests to test random selection
    requests = [Request(url=f"http://example.com/{i}") for i in range(30)]
    results = [await middleware.process_request(req, Spider()) for req in requests]

    # All assigned proxies should be from our list
    assigned_proxies = [r.meta["proxy"] for r in results]
    assert all(p in middleware.proxies for p in assigned_proxies)

    # With random selection, we should see some variation
    unique_proxies = set(assigned_proxies)
    assert len(unique_proxies) > 1


def test_proxy_middleware_validation_errors(tmp_path):
    from silkworm.middlewares import ProxyMiddleware

    # Must provide either proxies or proxy_file
    with pytest.raises(ValueError, match="Must provide either"):
        ProxyMiddleware()

    # Cannot provide both proxies and proxy_file
    proxy_file = tmp_path / "proxies.txt"
    proxy_file.write_text("http://proxy1:8080\n")

    with pytest.raises(ValueError, match="Cannot specify both"):
        ProxyMiddleware(proxies=["http://proxy1:8080"], proxy_file=proxy_file)

    # File must exist
    with pytest.raises(FileNotFoundError):
        ProxyMiddleware(proxy_file="nonexistent.txt")

    # File must contain at least one proxy
    empty_file = tmp_path / "empty.txt"
    empty_file.write_text("")

    with pytest.raises(ValueError, match="requires at least one proxy"):
        ProxyMiddleware(proxy_file=empty_file)

    # Empty proxies list should raise error
    with pytest.raises(ValueError, match="requires at least one proxy"):
        ProxyMiddleware(proxies=[])
