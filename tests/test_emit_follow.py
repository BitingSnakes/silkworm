"""Tests for the push-style ``emit``/``follow`` callback API."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest

from silkworm import Engine, HTMLResponse, Request, Response, Spider, SpiderError
from silkworm._scope import CrawlScope, enter_scope
from silkworm._types import JSONLike


@asynccontextmanager
async def recording_scope(
    response: Response | None = None,
) -> AsyncIterator[tuple[list[JSONLike], list[Request]]]:
    items: list[JSONLike] = []
    requests: list[Request] = []

    async def emit_item(item: JSONLike) -> None:
        items.append(item)

    async def schedule_request(request: Request) -> None:
        requests.append(request)

    scope = CrawlScope(
        owner="test",
        emit_item=emit_item,
        schedule_request=schedule_request,
        response=response,
    )
    async with enter_scope(scope):
        yield items, requests


async def _noop(response: Response) -> None:
    return None


def _response(cls: type[Response] = Response) -> Response:
    request = Request(url="http://example.com/dir/page", callback=_noop)
    return cls(url=request.url, status=200, headers={}, body=b"", request=request)


def _ok_client_fetch(engine: Engine) -> None:
    async def fake_fetch(req: Request) -> Response:
        return Response(url=req.url, status=200, headers={}, body=b"", request=req)

    engine.http.fetch = fake_fetch  # type: ignore[method-assign]


async def test_response_follow_schedules_joined_url_and_inherits_callback() -> None:
    response = _response()
    async with recording_scope(response) as (_, requests):
        await response.follow("next", meta={"page": 2})

    assert [req.url for req in requests] == ["http://example.com/dir/next"]
    assert requests[0].callback is _noop
    assert requests[0].meta == {"page": 2}


async def test_html_response_follow_schedules_requests() -> None:
    response = _response(HTMLResponse)
    async with recording_scope(response) as (_, requests):
        await response.follow("next")

    assert [req.url for req in requests] == ["http://example.com/dir/next"]


async def test_response_follow_all_skips_none_and_keeps_order() -> None:
    response = _response()

    async def other(resp: Response) -> None:
        return None

    async with recording_scope(response) as (_, requests):
        await response.follow_all(["next", None, "../up"], callback=other)

    assert [req.url for req in requests] == [
        "http://example.com/dir/next",
        "http://example.com/up",
    ]
    assert all(req.callback is other for req in requests)


async def test_spider_follow_resolves_urls_against_current_response() -> None:
    spider = Spider()
    response = _response()
    ready = Request(url="http://example.org/ready")
    async with recording_scope(response) as (_, requests):
        await spider.follow("next")
        await spider.follow(ready)
        await spider.follow_all(["a", None, "/b"])

    assert [req.url for req in requests] == [
        "http://example.com/dir/next",
        "http://example.org/ready",
        "http://example.com/dir/a",
        "http://example.com/b",
    ]
    assert requests[0].callback is _noop
    assert requests[1] is ready


async def test_spider_follow_url_without_response_is_used_verbatim() -> None:
    spider = Spider()
    async with recording_scope() as (_, requests):
        await spider.follow("http://example.com/seed", priority=5)

    assert requests[0].url == "http://example.com/seed"
    assert requests[0].priority == 5
    assert requests[0].callback is None


async def test_spider_follow_rejects_fields_with_request_target() -> None:
    spider = Spider()
    async with recording_scope():
        with pytest.raises(TypeError, match="only with a URL"):
            await spider.follow(Request(url="http://example.com"), priority=1)


async def test_spider_emit_forwards_items_and_rejects_requests() -> None:
    spider = Spider()
    async with recording_scope() as (items, _):
        await spider.emit({"title": "a"})
        with pytest.raises(TypeError, match="use follow"):
            await spider.emit(Request(url="http://example.com"))  # type: ignore[arg-type]

    assert items == [{"title": "a"}]


async def test_emit_and_follow_outside_engine_scope_raise() -> None:
    spider = Spider()
    with pytest.raises(SpiderError, match=r"emit\(\) can only be awaited"):
        await spider.emit({"x": 1})
    with pytest.raises(SpiderError, match=r"follow\(\) can only be awaited"):
        await spider.follow("http://example.com")
    with pytest.raises(SpiderError, match=r"follow\(\) can only be awaited"):
        await _response().follow("next")


async def test_default_start_requests_follows_start_urls() -> None:
    spider = Spider(start_urls=["http://example.com/a", "http://example.com/b"])
    async with recording_scope() as (_, requests):
        await spider.start_requests()

    assert [req.url for req in requests] == [
        "http://example.com/a",
        "http://example.com/b",
    ]
    assert all(req.callback == spider.parse for req in requests)


async def test_engine_streams_items_to_pipelines_while_callback_runs() -> None:
    events: list[str] = []

    class Pipeline:
        async def open(self, spider: Spider) -> None:
            return None

        async def close(self, spider: Spider) -> None:
            return None

        async def process_item(self, item: Any, spider: Spider) -> Any:
            events.append(f"pipeline:{item['n']}")
            return item

    class StreamingSpider(Spider):
        name = "streaming"
        start_urls = ("http://example.com/",)

        async def parse(self, response: Response) -> None:
            for n in range(2):
                await self.emit({"n": n})
                events.append(f"emitted:{n}")

    engine = Engine(StreamingSpider(), concurrency=1, item_pipelines=[Pipeline()])
    _ok_client_fetch(engine)
    await engine.run()

    assert events == ["pipeline:0", "emitted:0", "pipeline:1", "emitted:1"]
    assert engine._stats["items_scraped"] == 2


async def test_engine_follow_crawls_and_dedupes_requests() -> None:
    fetched: list[str] = []

    class FollowSpider(Spider):
        name = "follow"
        start_urls = ("http://example.com/",)

        async def parse(self, response: Response) -> None:
            if response.url == "http://example.com/":
                await response.follow("a")
                await response.follow("a")
                await self.follow("http://example.com/b", callback=self.parse_detail)

        async def parse_detail(self, response: Response) -> None:
            await self.emit({"detail": response.url})

    items: list[Any] = []
    engine = Engine(FollowSpider(), concurrency=1)

    async def fake_fetch(req: Request) -> Response:
        fetched.append(req.url)
        return Response(url=req.url, status=200, headers={}, body=b"", request=req)

    async def fake_process_item(item: Any) -> None:
        items.append(item)

    engine.http.fetch = fake_fetch  # type: ignore[method-assign]
    engine._process_item = fake_process_item  # type: ignore[method-assign]
    await engine.run()

    assert fetched == [
        "http://example.com/",
        "http://example.com/a",
        "http://example.com/b",
    ]
    assert items == [{"detail": "http://example.com/b"}]


async def test_task_group_children_inherit_the_callback_scope() -> None:
    class FanOutSpider(Spider):
        name = "fan-out"
        start_urls = ("http://example.com/",)

        async def parse(self, response: Response) -> None:
            async with asyncio.TaskGroup() as tg:
                for n in range(3):
                    tg.create_task(self.emit({"n": n}))

    items: list[Any] = []
    engine = Engine(FanOutSpider(), concurrency=1)
    _ok_client_fetch(engine)

    async def fake_process_item(item: Any) -> None:
        items.append(item)

    engine._process_item = fake_process_item  # type: ignore[method-assign]
    await engine.run()

    assert sorted(item["n"] for item in items) == [0, 1, 2]


async def test_emit_after_callback_finished_raises() -> None:
    leaked: list[asyncio.Task[None]] = []

    class LeakySpider(Spider):
        async def parse(self, response: Response) -> None:
            leaked.append(asyncio.create_task(self._late_emit()))

        async def _late_emit(self) -> None:
            await asyncio.sleep(0)
            await self.emit({"late": True})

    spider = LeakySpider()
    engine = Engine(spider)
    request = Request(url="http://example.com", callback=spider.parse)
    try:
        await engine._handle_response(
            Response(request.url, 200, {}, b"", request),
        )
        with pytest.raises(SpiderError, match="after callback 'parse' finished"):
            await leaked[0]
    finally:
        await engine.http.close()


async def test_in_flight_emit_from_detached_task_is_drained() -> None:
    events: list[str] = []
    detached: list[asyncio.Task[None]] = []

    class SlowPipeline:
        async def open(self, spider: Spider) -> None:
            return None

        async def close(self, spider: Spider) -> None:
            return None

        async def process_item(self, item: Any, spider: Spider) -> Any:
            await asyncio.sleep(0.05)
            events.append("processed")
            return item

    class DetachedSpider(Spider):
        async def parse(self, response: Response) -> None:
            detached.append(asyncio.create_task(self.emit({"late": True})))
            await asyncio.sleep(0)  # the task is now inside emit()

    spider = DetachedSpider()
    engine = Engine(spider, item_pipelines=[SlowPipeline()])
    request = Request(url="http://example.com", callback=spider.parse)
    try:
        await engine._handle_response(Response(request.url, 200, {}, b"", request))
        events.append("response handled")
        await detached[0]
    finally:
        await engine.http.close()

    assert events == ["processed", "response handled"]


@pytest.mark.parametrize(
    ("body", "match"),
    [
        ("generator", "is an async generator"),
        ("returns", "returned a dict"),
        ("sync", "must be an async function"),
    ],
)
async def test_engine_rejects_legacy_callback_shapes(body: str, match: str) -> None:
    async def generator_callback(response: Response) -> Any:
        yield {"legacy": True}

    async def returning_callback(response: Response) -> Any:
        return {"legacy": True}

    def sync_callback(response: Response) -> Any:
        return None

    callbacks: dict[str, Any] = {
        "generator": generator_callback,
        "returns": returning_callback,
        "sync": sync_callback,
    }
    request = Request(url="http://example.com", callback=callbacks[body])
    engine = Engine(Spider())
    try:
        with pytest.raises(SpiderError, match=match):
            await engine._handle_response(
                Response(request.url, 200, {}, b"", request),
            )
    finally:
        await engine.http.close()


async def test_pipeline_errors_surface_at_emit_call_site() -> None:
    caught: list[str] = []

    class FailingPipeline:
        async def open(self, spider: Spider) -> None:
            return None

        async def close(self, spider: Spider) -> None:
            return None

        async def process_item(self, item: Any, spider: Spider) -> Any:
            raise ValueError("bad item")

    class CatchingSpider(Spider):
        name = "catching"
        start_urls = ("http://example.com/",)

        async def parse(self, response: Response) -> None:
            try:
                await self.emit({"x": 1})
            except ValueError as exc:
                caught.append(str(exc))

    engine = Engine(CatchingSpider(), concurrency=1, item_pipelines=[FailingPipeline()])
    _ok_client_fetch(engine)
    await engine.run()

    assert caught == ["bad item"]
