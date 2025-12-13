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
