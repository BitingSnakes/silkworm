import asyncio

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

    assert engine.max_pending_requests == 30  # concurrency * 10


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

    assert engine.max_pending_requests == 2
    assert engine._queue.empty()


async def test_engine_uses_custom_dedup_key(monkeypatch: pytest.MonkeyPatch):
    class ParamsSpider(Spider):
        name = "params"

        async def start_requests(self):
            await self.follow(
                Request(
                    url="http://example.com/search",
                    params={"page": 1},
                    callback=self.parse,
                )
            )
            await self.follow(
                Request(
                    url="http://example.com/search",
                    params={"page": 2},
                    callback=self.parse,
                )
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
    # Both custom keys are distinct, so both requests were recorded and fetched.
    assert len(engine._seen) == 2


async def test_engine_dequeues_higher_priority_requests_first(
    monkeypatch: pytest.MonkeyPatch,
):
    class PrioritySpider(Spider):
        name = "priority"

        async def start_requests(self):
            await self.follow(Request("http://example.com/seed", callback=self.parse))

        async def parse(self, response):
            if response.url != "http://example.com/seed":
                return

            await self.follow(
                Request(
                    "http://example.com/low",
                    callback=self.parse,
                    priority=-10,
                )
            )
            await self.follow(
                Request(
                    "http://example.com/high-a",
                    callback=self.parse,
                    priority=10,
                )
            )
            await self.follow(
                Request(
                    "http://example.com/high-b",
                    callback=self.parse,
                    priority=10,
                )
            )
            await self.follow(
                Request(
                    "http://example.com/default",
                    callback=self.parse,
                )
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
                await self.follow(
                    Request(
                        url=f"http://example.com/{i}",
                        callback=self.parse,
                        dont_filter=True,
                    )
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
            await self.follow(Request(url="http://example.com", callback=self.parse))

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
            await self.follow(
                Request(
                    url="http://example.com",
                    callback=self.parse,
                    errback=self.handle_error,
                )
            )

        async def parse(self, response):
            return None

        async def handle_error(self, request: Request, exception: Exception):
            await self.emit(
                {
                    "url": request.url,
                    "error_type": exception.__class__.__name__,
                }
            )

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


def _track_max_queue_size(engine: Engine) -> list[int]:
    """Record the largest queue size observed after every enqueue."""
    observed = [0]
    put_nowait = engine._queue.put_nowait

    def tracking_put_nowait(entry):
        put_nowait(entry)
        observed[0] = max(observed[0], engine._queue.qsize())

    engine._queue.put_nowait = tracking_put_nowait  # type: ignore[method-assign]
    return observed


def _ok_fetch(engine: Engine, fetched: list[str] | None = None) -> None:
    async def fake_fetch(req: Request) -> Response:
        if fetched is not None:
            fetched.append(req.url)
        return Response(url=req.url, status=200, headers={}, body=b"", request=req)

    engine.http.fetch = fake_fetch  # type: ignore[method-assign]


async def test_single_worker_fan_out_past_full_queue_does_not_deadlock():
    class FanOut(Spider):
        start_urls = ("http://example.com/",)

        async def parse(self, response: Response) -> None:
            if response.url == "http://example.com/":
                for i in range(20):
                    await self.follow(f"http://example.com/{i}")

    fetched: list[str] = []
    engine = Engine(FanOut(), concurrency=1, max_pending_requests=2)
    _ok_fetch(engine, fetched)

    async with asyncio.timeout(5):
        await engine.run()

    assert len(fetched) == 21
    assert engine._stalled_workers == {}
    assert not engine._capacity_waiters


async def test_all_workers_fanning_out_do_not_deadlock():
    class NestedFanOut(Spider):
        start_urls = tuple(f"http://example.com/{i}" for i in range(3))

        async def parse(self, response: Response) -> None:
            depth = response.url.count("/") - 2
            if depth < 3:
                async with asyncio.TaskGroup() as tg:
                    for i in range(4):
                        tg.create_task(self.follow(f"{response.url}/{i}"))

    fetched: list[str] = []
    engine = Engine(NestedFanOut(), concurrency=3, max_pending_requests=2)
    _ok_fetch(engine, fetched)

    async with asyncio.timeout(5):
        await engine.run()

    # 3 seeds, each expanding 4-way twice more: 3 * (1 + 4 + 16).
    assert len(fetched) == 63
    assert len(set(fetched)) == 63


async def test_start_requests_never_exceed_max_pending_requests():
    class ManySeeds(Spider):
        start_urls = tuple(f"http://example.com/{i}" for i in range(50))

        async def parse(self, response: Response) -> None:
            return None

    fetched: list[str] = []
    engine = Engine(ManySeeds(), concurrency=2, max_pending_requests=3)
    observed = _track_max_queue_size(engine)
    _ok_fetch(engine, fetched)

    async with asyncio.timeout(5):
        await engine.run()

    assert len(fetched) == 50
    assert observed[0] <= 3


async def test_callbacks_keep_backpressure_while_another_worker_progresses():
    class OneFanOut(Spider):
        start_urls = ("http://example.com/",)

        async def parse(self, response: Response) -> None:
            if response.url == "http://example.com/":
                for i in range(30):
                    await self.follow(f"http://example.com/leaf/{i}")
            else:
                await asyncio.sleep(0)  # leaf pages never schedule requests

    fetched: list[str] = []
    engine = Engine(OneFanOut(), concurrency=2, max_pending_requests=2)
    observed = _track_max_queue_size(engine)
    _ok_fetch(engine, fetched)

    async with asyncio.timeout(5):
        await engine.run()

    assert len(fetched) == 31
    # The idle worker keeps draining, so the fan-out waits instead of overflowing.
    assert observed[0] <= 2


async def test_growing_frontier_keeps_all_workers_busy_when_queue_is_full():
    # Every page schedules more links than one fetch consumes, so the queue
    # stays over the bound. Workers must keep crawling instead of parking.
    class Growing(Spider):
        start_urls = ("http://example.com/r",)

        async def parse(self, response: Response) -> None:
            if response.url.count("/") < 7:
                for i in range(3):
                    await self.follow(f"{response.url}/{i}")

    engine = Engine(Growing(), concurrency=8, max_pending_requests=4)
    in_flight = 0
    samples: list[int] = []

    async def fake_fetch(req: Request) -> Response:
        nonlocal in_flight
        in_flight += 1
        samples.append(in_flight)
        await asyncio.sleep(0.005)
        in_flight -= 1
        return Response(url=req.url, status=200, headers={}, body=b"", request=req)

    engine.http.fetch = fake_fetch  # type: ignore[method-assign]
    async with asyncio.timeout(10):
        await engine.run()

    assert len(samples) == 1 + 3 + 9 + 27 + 81
    # With parked workers this averaged ~2 of 8; now it stays close to 8.
    assert sum(samples) / len(samples) > 5


async def test_sole_heavy_producer_is_throttled_by_many_consumers():
    class Sitemap(Spider):
        start_urls = ("http://example.com/sitemap",)

        async def parse(self, response: Response) -> None:
            if response.url.endswith("/sitemap"):
                for i in range(100):
                    await self.follow(f"http://example.com/p/{i}")

    fetched: list[str] = []
    engine = Engine(Sitemap(), concurrency=8, max_pending_requests=5)
    observed = _track_max_queue_size(engine)
    _ok_fetch(engine, fetched)

    async with asyncio.timeout(5):
        await engine.run()

    assert len(fetched) == 101
    assert observed[0] <= 5


async def test_cancelled_crawl_leaves_no_capacity_waiters():
    class Endless(Spider):
        start_urls = ("http://example.com",)

        async def parse(self, response: Response) -> None:
            for i in range(5):
                await self.follow(f"{response.url}/{i}")

    engine = Engine(Endless(), concurrency=3, max_pending_requests=2)

    async def slow_fetch(req: Request) -> Response:
        await asyncio.sleep(0.001)
        return Response(url=req.url, status=200, headers={}, body=b"", request=req)

    engine.http.fetch = slow_fetch  # type: ignore[method-assign]
    task = asyncio.create_task(engine.run())
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert engine._stalled_workers == {}
    assert not engine._capacity_waiters
