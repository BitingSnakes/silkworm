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


def test_engine_rejects_non_positive_concurrency():
    with pytest.raises(ValueError, match="concurrency must be positive"):
        Engine(SmallSpider(), concurrency=0)


def test_engine_rejects_non_positive_max_pending_requests():
    with pytest.raises(ValueError, match="max_pending_requests must be positive"):
        Engine(SmallSpider(), max_pending_requests=0)


def test_engine_rejects_non_positive_http_client_concurrency():
    class BadHttpClient:
        concurrency = 0
        html_max_size_bytes = 5_000_000

        async def fetch(self, request: Request) -> Response:
            return Response(
                url=request.url,
                status=200,
                headers={},
                body=b"",
                request=request,
            )

        async def close(self) -> None:
            return None

    with pytest.raises(ValueError, match=r"http_client\.concurrency must be positive"):
        Engine(SmallSpider(), http_client=BadHttpClient())  # type: ignore[arg-type]


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


async def test_engine_uses_custom_dedup_key(monkeypatch: pytest.MonkeyPatch):
    class ParamsSpider(Spider):
        name = "params"

        async def start_requests(self):
            yield Request(
                url="http://example.com/search",
                params={"page": 1},
                callback=self.parse,
            )
            yield Request(
                url="http://example.com/search",
                params={"page": 2},
                callback=self.parse,
            )

        async def parse(self, response):
            return None

    spider = ParamsSpider()
    fetched_urls: list[str] = []
    engine = Engine(
        spider,
        concurrency=1,
        dedup_key=lambda req: f"{req.url}:{req.params.get('page')}",
    )

    async def fake_fetch(req: Request) -> Response:
        fetched_urls.append(req.url)
        return Response(url=req.url, status=200, headers={}, body=b"", request=req)

    monkeypatch.setattr(engine.http, "fetch", fake_fetch)

    await engine.run()

    assert fetched_urls == [
        "http://example.com/search",
        "http://example.com/search",
    ]
    assert engine._seen == {
        "http://example.com/search:1",
        "http://example.com/search:2",
    }


async def test_engine_dequeues_higher_priority_requests_first(
    monkeypatch: pytest.MonkeyPatch,
):
    class PrioritySpider(Spider):
        name = "priority"

        async def start_requests(self):
            yield Request("http://example.com/seed", callback=self.parse)

        async def parse(self, response):
            if response.url != "http://example.com/seed":
                return

            yield Request(
                "http://example.com/low",
                callback=self.parse,
                priority=-10,
            )
            yield Request(
                "http://example.com/high-a",
                callback=self.parse,
                priority=10,
            )
            yield Request(
                "http://example.com/high-b",
                callback=self.parse,
                priority=10,
            )
            yield Request(
                "http://example.com/default",
                callback=self.parse,
            )

    fetched_urls: list[str] = []
    engine = Engine(PrioritySpider(), concurrency=1)

    async def fake_fetch(req: Request) -> Response:
        fetched_urls.append(req.url)
        return Response(url=req.url, status=200, headers={}, body=b"", request=req)

    monkeypatch.setattr(engine.http, "fetch", fake_fetch)

    await engine.run()

    assert fetched_urls == [
        "http://example.com/seed",
        "http://example.com/high-a",
        "http://example.com/high-b",
        "http://example.com/default",
        "http://example.com/low",
    ]


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
