from urllib.parse import parse_qsl, urlsplit

import pytest

from silkworm.http import HttpClient
from silkworm.middlewares import RetryMiddleware
from silkworm.request import Request
from silkworm.response import HTMLResponse, Response
from silkworm import response as response_module
from silkworm.spiders import Spider


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


def test_htmlresponse_doc_is_cached(monkeypatch: pytest.MonkeyPatch):
    class CountingDocument:
        instances = 0

        def __init__(self, html: str) -> None:
            type(self).instances += 1
            self.html = html

        def select(self, selector: str):
            return [selector]

    monkeypatch.setattr(response_module, "Document", CountingDocument)

    resp = HTMLResponse(
        url="http://example.com",
        status=200,
        headers={},
        body=b"<html></html>",
        request=Request(url="http://example.com"),
    )

    first = resp.doc
    second = resp.doc

    assert first is second
    assert CountingDocument.instances == 1
    assert first.select("body") == ["body"]


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


@pytest.mark.anyio("asyncio")
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
    assert sleep_calls == [0.1]


@pytest.mark.anyio("asyncio")
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
