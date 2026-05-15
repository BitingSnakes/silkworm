import pytest

from silkworm.engine import Engine
from silkworm.request import Request
from silkworm.response import Response
from silkworm.spiders import Spider


class SmallSpider(Spider):
    name = "small"
    start_urls: tuple[str, ...] = tuple(f"http://example.com/{i}" for i in range(5))

    async def parse(self, response):
        return None


def test_engine_defaults_to_bounded_queue():
    spider = SmallSpider()
    engine = Engine(spider, concurrency=3)

    assert engine._queue.maxsize == 30  # concurrency * 10


async def test_engine_runs_with_limited_queue(monkeypatch: pytest.MonkeyPatch):
    spider = SmallSpider()
    engine = Engine(spider, concurrency=2, max_pending_requests=2)

    async def fake_fetch(req: Request) -> Response:
        return Response(
            url=req.url,
            status=200,
            headers={},
            body=b"",
            request=req,
        )

    monkeypatch.setattr(engine.http, "fetch", fake_fetch)

    await engine.run()

    assert engine._queue.maxsize == 2
    assert engine._queue.empty()


async def test_engine_does_not_track_dont_filter_requests(
    monkeypatch: pytest.MonkeyPatch,
):
    class NoFilterSpider(Spider):
        name = "nofilter"

        async def start_requests(self):
            for i in range(3):
                yield Request(
                    url=f"http://example.com/{i}",
                    callback=self.parse,
                    dont_filter=True,
                )

        async def parse(self, response):
            return None

    spider = NoFilterSpider()
    engine = Engine(spider, concurrency=1)

    async def fake_fetch(req: Request) -> Response:
        return Response(url=req.url, status=200, headers={}, body=b"", request=req)

    monkeypatch.setattr(engine.http, "fetch", fake_fetch)

    await engine.run()

    assert engine._seen == set()


async def test_engine_calls_exception_middleware_from_response_middlewares(
    monkeypatch: pytest.MonkeyPatch,
):
    class RetryFromExceptionMiddleware:
        def __init__(self) -> None:
            self.calls = 0

        async def process_exception(
            self,
            request: Request,
            exception: Exception,
            spider: Spider,
        ) -> Request | None:
            self.calls += 1
            return request.replace(dont_filter=True, meta={"retried": True})

        async def process_response(
            self,
            response: Response,
            spider: Spider,
        ) -> Response | Request:
            return response

    class OneRequestSpider(Spider):
        name = "exception-middleware"

        async def start_requests(self):
            yield Request(url="http://example.com", callback=self.parse)

        async def parse(self, response):
            return None

    middleware = RetryFromExceptionMiddleware()
    engine = Engine(
        OneRequestSpider(),
        concurrency=1,
        response_middlewares=[middleware],
    )
    attempts = 0

    async def fake_fetch(req: Request) -> Response:
        nonlocal attempts
        attempts += 1
        if not req.meta.get("retried"):
            raise RuntimeError("temporary fetch failure")
        return Response(url=req.url, status=200, headers={}, body=b"", request=req)

    monkeypatch.setattr(engine.http, "fetch", fake_fetch)

    await engine.run()

    assert attempts == 2
    assert middleware.calls == 1


async def test_engine_runs_request_errback_for_unhandled_exception(
    monkeypatch: pytest.MonkeyPatch,
):
    scraped_items: list[dict[str, str]] = []

    class ErrbackSpider(Spider):
        name = "errback"

        async def start_requests(self):
            yield Request(
                url="http://example.com",
                callback=self.parse,
                errback=self.handle_error,
            )

        async def parse(self, response):
            return None

        async def handle_error(self, request: Request, exception: Exception):
            yield {
                "url": request.url,
                "error_type": exception.__class__.__name__,
            }

    engine = Engine(ErrbackSpider(), concurrency=1)

    async def fake_fetch(req: Request) -> Response:
        raise RuntimeError("fetch failed")

    async def fake_process_item(item):
        assert isinstance(item, dict)
        scraped_items.append(item)

    monkeypatch.setattr(engine.http, "fetch", fake_fetch)
    monkeypatch.setattr(engine, "_process_item", fake_process_item)

    await engine.run()

    assert scraped_items == [
        {
            "url": "http://example.com",
            "error_type": "RuntimeError",
        }
    ]
    assert engine._stats["errors"] == 1
