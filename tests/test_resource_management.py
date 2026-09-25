"""Failure-injection tests for deterministic framework resource cleanup."""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest

from silkworm import Engine, HTMLResponse, Request, Response, Spider
from silkworm._middlewares.stream import RequestResponseStreamMiddleware
from silkworm.cdp import CDPClient
from silkworm.exceptions import SpiderError
from silkworm.http import HttpClient
from silkworm.onionlink import OnionLinkClient
from silkworm.servo import ServoFetchClient


class _CloseTrackingHttpClient:
    concurrency = 1
    html_max_size_bytes = 5_000_000

    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def fetch(self, request: Request) -> Response:
        return Response(request.url, 200, {}, b"", request)

    async def close(self) -> None:
        self.events.append("close:http")


async def test_engine_closes_lifecycle_components_in_reverse_order() -> None:
    events: list[str] = []

    class LifecycleMiddleware:
        async def open(self, spider: Spider) -> None:
            events.append("open:middleware")

        async def close(self, spider: Spider) -> None:
            events.append("close:middleware")

        async def process_request(self, request: Request, spider: Spider) -> Request:
            return request

    class LifecyclePipeline:
        async def open(self, spider: Spider) -> None:
            events.append("open:pipeline")

        async def close(self, spider: Spider) -> None:
            events.append("close:pipeline")

        async def process_item(self, item: object, spider: Spider) -> object:
            return item

    class LifecycleSpider(Spider):
        async def open(self) -> None:
            events.append("open:spider")

        async def close(self) -> None:
            events.append("close:spider")

    engine = Engine(
        LifecycleSpider(),
        http_client=cast("Any", _CloseTrackingHttpClient(events)),
        request_middlewares=[LifecycleMiddleware()],
        item_pipelines=[cast("Any", LifecyclePipeline())],
    )

    await engine.run()

    assert events == [
        "open:middleware",
        "open:spider",
        "open:pipeline",
        "close:pipeline",
        "close:spider",
        "close:middleware",
        "close:http",
    ]


async def test_engine_preserves_startup_error_and_attempts_every_rollback() -> None:
    events: list[str] = []

    class Middleware:
        async def open(self, spider: Spider) -> None:
            events.append("open:middleware")

        async def close(self, spider: Spider) -> None:
            events.append("close:middleware")

        async def process_request(self, request: Request, spider: Spider) -> Request:
            return request

    class Pipeline:
        def __init__(self, name: str, *, fail_close: bool = False) -> None:
            self.name = name
            self.fail_close = fail_close

        async def open(self, spider: Spider) -> None:
            events.append(f"open:{self.name}")

        async def close(self, spider: Spider) -> None:
            events.append(f"close:{self.name}")
            if self.fail_close:
                raise RuntimeError(f"{self.name} cleanup failed")

        async def process_item(self, item: object, spider: Spider) -> object:
            return item

    class FailingSpider(Spider):
        async def open(self) -> None:
            events.append("open:spider")

        async def close(self) -> None:
            events.append("close:spider")

        async def start_requests(self):
            if False:
                yield Request("https://example.com")
            raise RuntimeError("startup failed")

    engine = Engine(
        FailingSpider(),
        request_middlewares=[Middleware()],
        item_pipelines=[
            cast("Any", Pipeline("first")),
            cast("Any", Pipeline("second", fail_close=True)),
        ],
    )
    try:
        with pytest.raises(RuntimeError, match="startup failed") as error:
            await engine.open_spider()
    finally:
        await engine.http.close()

    assert events == [
        "open:middleware",
        "open:spider",
        "open:first",
        "open:second",
        "close:second",
        "close:first",
        "close:spider",
        "close:middleware",
    ]
    assert any("second cleanup failed" in note for note in error.value.__notes__)


async def test_callback_failure_closes_response() -> None:
    spider = Spider()

    def fail(response: Response) -> None:
        raise ValueError("callback failed")

    request = Request("https://example.com", callback=fail)
    response = Response(request.url, 200, {}, b"payload", request)
    engine = Engine(spider)
    try:
        with pytest.raises(SpiderError, match="callback 'fail' failed"):
            await engine._handle_response(response)
    finally:
        await engine.http.close()

    assert response._closed is True
    assert response.body == b""


async def test_response_middleware_replacements_close_on_failure() -> None:
    replacement: Response | None = None

    class Replace:
        async def process_response(
            self, response: Response, spider: Spider
        ) -> Response:
            nonlocal replacement
            replacement = Response(
                response.url,
                response.status,
                dict(response.headers),
                response.body,
                response.request,
            )
            return replacement

    class Fail:
        async def process_response(
            self, response: Response, spider: Spider
        ) -> Response:
            raise RuntimeError("middleware failed")

    request = Request("https://example.com")
    original = Response(request.url, 200, {}, b"payload", request)
    engine = Engine(Spider(), response_middlewares=[Replace(), Fail()])
    try:
        with pytest.raises(RuntimeError, match="middleware failed"):
            await engine._handle_response(original)
    finally:
        await engine.http.close()

    assert original._closed is True
    assert replacement is not None and replacement._closed is True


def test_html_response_context_closes_base_state_when_document_close_fails() -> None:
    class BrokenDocument:
        def close(self) -> None:
            raise RuntimeError("document close failed")

    request = Request("https://example.com")
    response = HTMLResponse(request.url, 200, {}, b"payload", request)
    response._document = cast("Any", BrokenDocument())

    with pytest.raises(RuntimeError, match="document close failed"), response:
        pass

    assert response._closed is True
    assert response.body == b""
    assert response._document is None


async def test_http_client_async_context_closes_transport() -> None:
    class Transport:
        def __init__(self) -> None:
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    client = HttpClient()
    transport = Transport()
    client._client = cast("Any", transport)

    async with client as entered:
        assert entered is client

    assert transport.closed is True


async def test_client_context_preserves_body_error_when_cleanup_fails() -> None:
    class Transport:
        async def close(self) -> None:
            raise RuntimeError("transport cleanup failed")

    client = HttpClient()
    client._client = cast("Any", Transport())

    with pytest.raises(ValueError, match="body failed") as error:
        async with client:
            raise ValueError("body failed")

    assert any("transport cleanup failed" in note for note in error.value.__notes__)


async def test_cdp_context_connects_and_closes() -> None:
    events: list[str] = []
    client = object.__new__(CDPClient)

    async def connect() -> None:
        events.append("connect")

    async def close() -> None:
        events.append("close")

    client.connect = connect  # type: ignore[method-assign]
    client.close = close  # type: ignore[method-assign]
    async with client as entered:
        assert entered is client

    assert events == ["connect", "close"]


@pytest.mark.parametrize("client_type", [ServoFetchClient, OnionLinkClient])
async def test_initialized_client_context_closes(
    client_type: type[ServoFetchClient | OnionLinkClient],
) -> None:
    client = object.__new__(client_type)
    closed = False

    async def close() -> None:
        nonlocal closed
        closed = True

    client.close = close  # type: ignore[method-assign]
    async with client as entered:
        assert entered is client

    assert closed is True


async def test_stream_close_does_not_hang_after_sender_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Client:
        def __init__(self) -> None:
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    client = Client()
    monkeypatch.setattr(
        "silkworm._middlewares.stream.Client",
        lambda: client,
    )
    middleware = RequestResponseStreamMiddleware(
        "https://collector.example.com/events",
        queue_size=1,
    )
    spider = Spider()
    await middleware.open(spider)
    original_task = middleware._sender_task
    assert original_task is not None
    original_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await original_task

    async def fail_sender() -> None:
        raise RuntimeError("sender failed")

    middleware._sender_task = asyncio.create_task(fail_sender())
    await asyncio.sleep(0)
    assert middleware._queue is not None
    middleware._queue.put_nowait({"event": "queued"})

    with pytest.raises(RuntimeError, match="sender failed"):
        async with asyncio.timeout(1):
            await middleware.close(spider)

    assert client.closed is True
    assert middleware._sender_task is None
    assert middleware._queue is None
