"""Engine production features: results, failure policy, limits, filtering,
graceful stop, per-domain concurrency, deduplication, job persistence, metrics,
and the IgnoreRequest/DropItem/CloseSpider control-flow exceptions."""

from __future__ import annotations

import asyncio
from collections import Counter
from pathlib import Path
from typing import Any, cast

import pytest

from silkworm import (
    CloseSpider,
    CrawlFailedError,
    CrawlResult,
    DropItem,
    Engine,
    IgnoreRequest,
    Request,
    Response,
    Spider,
    SpiderError,
)
from silkworm._jobs import JobState
from silkworm._stats import MAX_LABELS, OTHER_LABEL, CrawlStats


def site(links: dict[str, list[str]]):
    """Return a fake fetch serving ``links`` (URL -> outgoing links) as JSON-free bodies."""
    fetched: list[str] = []

    async def fetch(req: Request) -> Response:
        fetched.append(req.url)
        body = "\n".join(links.get(req.url, [])).encode()
        return Response(url=req.url, status=200, headers={}, body=body, request=req)

    return fetch, fetched


class LinkSpider(Spider):
    """Follows every line of the body as a link and emits one item per page."""

    name = "links"

    async def parse(self, response: Response) -> None:
        await self.emit({"url": response.url})
        for line in response.text.splitlines():
            if line:
                await self.follow(line)


def engine_for(spider: Spider, links: dict[str, list[str]], **options: Any):
    engine = Engine(spider, concurrency=options.pop("concurrency", 2), **options)
    fetch, fetched = site(links)
    engine.http.fetch = fetch  # type: ignore[method-assign]
    return engine, fetched


CHAIN = {
    "http://a.test/0": ["http://a.test/1"],
    "http://a.test/1": ["http://a.test/2"],
    "http://a.test/2": ["http://a.test/3"],
    "http://a.test/3": [],
}


# -- #1 crawl result and failure policy ---------------------------------------


async def test_run_returns_crawl_result_with_stats() -> None:
    engine, _ = engine_for(LinkSpider(start_urls=["http://a.test/0"]), CHAIN)
    result = await engine.run()

    assert isinstance(result, CrawlResult)
    assert result.ok and result.failures == ()
    assert result.close_reason == "finished"
    assert result.spider == "links"
    assert (result.requests_sent, result.responses_received) == (4, 4)
    assert result.items_scraped == 4
    assert result.labeled_stats["responses_by_status"] == {"200": 4}
    assert result.labeled_stats["requests_by_domain"] == {"a.test": 4}
    with pytest.raises(TypeError):
        result.stats["errors"] = 1  # type: ignore[index]


async def test_failure_policy_raises_with_result() -> None:
    class Broken(Spider):
        start_urls = tuple(f"http://a.test/{i}" for i in range(4))

        async def parse(self, response: Response) -> None:
            if not response.url.endswith("0"):
                raise ValueError("selectors changed")
            await self.emit({"ok": True})

    engine, _ = engine_for(Broken(), {}, max_error_rate=0.5, min_items=2)
    with pytest.raises(CrawlFailedError) as caught:
        await engine.run()

    result = caught.value.result
    assert result.errors == 3 and result.items_scraped == 1
    assert result.error_rate == 0.75
    assert len(result.failures) == 2
    assert "error rate 75.0% exceeds max_error_rate 50.0%" in result.failures[0]
    assert "fewer than min_items=2" in result.failures[1]
    assert result.labeled_stats["errors_by_type"] == {"ValueError": 3}


async def test_failure_policy_passes_within_limits() -> None:
    engine, _ = engine_for(
        LinkSpider(start_urls=["http://a.test/0"]),
        CHAIN,
        max_error_rate=0.0,
        min_items=4,
    )
    assert (await engine.run()).ok


async def test_drop_rate_policy_ignores_max_items_overflow() -> None:
    class Dropper:
        async def open(self, spider: Spider) -> None: ...
        async def close(self, spider: Spider) -> None: ...

        async def process_item(self, item: Any, spider: Spider) -> Any:
            if item["url"].endswith("3"):
                raise DropItem("bad", reason="invalid")
            return item

    engine, _ = engine_for(
        LinkSpider(start_urls=["http://a.test/0"]),
        CHAIN,
        item_pipelines=[Dropper()],
        max_item_drop_rate=0.2,
    )
    with pytest.raises(CrawlFailedError, match=r"item drop rate 25\.0%"):
        await engine.run()


def test_engine_rejects_invalid_limits() -> None:
    for kwargs, message in [
        ({"max_requests": 0}, "max_requests must be positive"),
        ({"max_depth": -1}, "max_depth must be non-negative"),
        ({"max_error_rate": 1.5}, "max_error_rate must be between"),
        ({"max_duration": 0}, "max_duration must be positive"),
        ({"concurrency_per_domain": 0}, "concurrency_per_domain must be positive"),
    ]:
        with pytest.raises(ValueError, match=message):
            Engine(Spider(), **kwargs)


# -- #3 stop limits and depth -----------------------------------------------------


async def test_max_depth_filters_deeper_links() -> None:
    engine, fetched = engine_for(
        LinkSpider(start_urls=["http://a.test/0"]), CHAIN, max_depth=2
    )
    result = await engine.run()

    assert fetched == ["http://a.test/0", "http://a.test/1", "http://a.test/2"]
    assert result.stats["depth_filtered"] == 1
    assert result.close_reason == "finished"


async def test_depth_is_recorded_in_request_meta() -> None:
    depths: dict[str, int] = {}

    class DepthSpider(LinkSpider):
        async def parse(self, response: Response) -> None:
            depths[response.url] = response.request.meta["depth"]  # type: ignore[assignment]
            await super().parse(response)

    engine, _ = engine_for(DepthSpider(start_urls=["http://a.test/0"]), CHAIN)
    await engine.run()
    assert depths == {f"http://a.test/{i}": i for i in range(4)}


async def test_max_requests_stops_after_exact_count() -> None:
    wide = {"http://a.test/": [f"http://a.test/{i}" for i in range(20)]}
    engine, fetched = engine_for(
        LinkSpider(start_urls=["http://a.test/"]), wide, max_requests=5
    )
    result = await engine.run()

    assert len(fetched) == 5
    assert result.close_reason == "max_requests"
    assert result.stats["dropped_requests"] > 0


async def test_max_items_is_exact_and_drops_overflow() -> None:
    class Many(Spider):
        start_urls = ("http://a.test/",)

        async def parse(self, response: Response) -> None:
            for i in range(10):
                await self.emit({"i": i})

    kept: list[Any] = []

    class Keep:
        async def open(self, spider: Spider) -> None: ...
        async def close(self, spider: Spider) -> None: ...

        async def process_item(self, item: Any, spider: Spider) -> Any:
            kept.append(item)
            return item

    engine, _ = engine_for(Many(), {}, item_pipelines=[Keep()], max_items=3)
    result = await engine.run()

    assert [item["i"] for item in kept] == [0, 1, 2]
    assert result.items_scraped == 3 and result.items_dropped == 7
    assert result.labeled_stats["items_dropped_by_reason"] == {"max_items": 7}
    assert result.close_reason == "max_items"


async def test_max_errors_stops_crawl() -> None:
    class Failing(Spider):
        start_urls = tuple(f"http://a.test/{i}" for i in range(10))

        async def parse(self, response: Response) -> None:
            raise RuntimeError("boom")

    engine, fetched = engine_for(Failing(), {}, concurrency=1, max_errors=2)
    result = await engine.run()

    assert result.close_reason == "max_errors"
    assert result.errors == 2
    assert len(fetched) == 2


async def test_max_duration_stops_crawl() -> None:
    class Slow(Spider):
        start_urls = tuple(f"http://a.test/{i}" for i in range(50))

        async def parse(self, response: Response) -> None:
            await asyncio.sleep(0.05)

    engine, fetched = engine_for(Slow(), {}, concurrency=1, max_duration=0.2)
    result = await engine.run()

    assert result.close_reason == "max_duration"
    assert 1 <= len(fetched) < 50


async def test_close_spider_from_callback() -> None:
    class Stopper(LinkSpider):
        async def parse(self, response: Response) -> None:
            await super().parse(response)
            if response.url.endswith("/1"):
                raise CloseSpider("found_target")

    engine, fetched = engine_for(Stopper(start_urls=["http://a.test/0"]), CHAIN)
    result = await engine.run()

    assert result.close_reason == "found_target"
    assert result.errors == 0
    assert "http://a.test/3" not in fetched


# -- #4 off-site filtering ----------------------------------------------------------


async def test_allowed_domains_filter_offsite_links() -> None:
    class Scoped(LinkSpider):
        allowed_domains = ("a.test",)

        async def parse(self, response: Response) -> None:
            await super().parse(response)
            if response.url == "http://a.test/":
                await self.follow("http://other.test/forced", dont_filter=True)

    links = {
        "http://a.test/": [
            "http://www.a.test/sub",
            "http://evil.test/",
            "http://nota.test/",
            "http://a.test.evil.test/",
        ]
    }
    engine, fetched = engine_for(Scoped(start_urls=["http://a.test/"]), links)
    result = await engine.run()

    assert sorted(fetched) == [
        "http://a.test/",
        "http://other.test/forced",
        "http://www.a.test/sub",
    ]
    assert result.stats["offsite_filtered"] == 3


# -- #5 graceful stop -------------------------------------------------------------------


async def test_stop_finishes_in_flight_and_discards_pending() -> None:
    events: list[str] = []
    closed = asyncio.Event()

    class Pipe:
        async def open(self, spider: Spider) -> None: ...

        async def close(self, spider: Spider) -> None:
            closed.set()

        async def process_item(self, item: Any, spider: Spider) -> Any:
            events.append(item["url"])
            return item

    class Stopping(Spider):
        start_urls = tuple(f"http://a.test/{i}" for i in range(20))

        async def parse(self, response: Response) -> None:
            if response.url == "http://a.test/0":
                engine.stop()
                await asyncio.sleep(0.01)  # in-flight work still completes
            await self.emit({"url": response.url})

    engine, fetched = engine_for(Stopping(), {}, concurrency=1, item_pipelines=[Pipe()])
    result = await engine.run()

    assert result.close_reason == "shutdown"
    assert fetched == ["http://a.test/0"]
    assert events == ["http://a.test/0"]
    assert closed.is_set()
    assert result.stats["dropped_requests"] >= 1


async def test_stop_aborts_long_start_requests() -> None:
    produced = 0

    class Endless(Spider):
        async def start_requests(self) -> None:
            nonlocal produced
            for i in range(10_000):
                produced += 1
                if i == 3:
                    engine.stop()
                await self.follow(f"http://a.test/{i}")

        async def parse(self, response: Response) -> None:
            return None

    engine, _ = engine_for(Endless(), {})
    result = await engine.run()

    assert result.close_reason == "shutdown"
    assert produced == 4  # the follow after stop() raised out of start_requests


async def test_stop_skips_failure_policy() -> None:
    class Stopper(Spider):
        start_urls = ("http://a.test/",)

        async def parse(self, response: Response) -> None:
            engine.stop()

    engine, _ = engine_for(Stopper(), {}, min_items=10)
    assert (await engine.run()).close_reason == "shutdown"


# -- control-flow exceptions --------------------------------------------------------


async def test_ignore_request_from_middleware_is_not_an_error() -> None:
    class Blocker:
        async def process_request(self, request: Request, spider: Spider) -> Request:
            if "blocked" in request.url:
                raise IgnoreRequest("nope", reason="policy")
            return request

    class Spy(Spider):
        start_urls = ("http://a.test/ok", "http://a.test/blocked")

        async def parse(self, response: Response) -> None:
            return None

        async def on_error(self, request: Request, exc: Exception) -> None:
            raise AssertionError("errback must not run for ignored requests")

    engine, fetched = engine_for(Spy(), {}, request_middlewares=[Blocker()])
    result = await engine.run()

    assert fetched == ["http://a.test/ok"]
    assert result.errors == 0
    assert result.stats["ignored_requests"] == 1
    assert result.labeled_stats["ignored_by_reason"] == {"policy": 1}


# -- #6 per-domain concurrency --------------------------------------------------------


async def test_concurrency_per_domain_limits_each_host() -> None:
    active: Counter[str] = Counter()
    peak: Counter[str] = Counter()

    class Multi(Spider):
        start_urls = tuple(
            f"http://{host}.test/{i}" for i in range(6) for host in ("a", "b")
        )

        async def parse(self, response: Response) -> None:
            return None

    engine = Engine(Multi(), concurrency=6, concurrency_per_domain=2)

    async def fetch(req: Request) -> Response:
        host = req.url.split("/")[2]
        active[host] += 1
        peak[host] = max(peak[host], active[host])
        await asyncio.sleep(0.02)
        active[host] -= 1
        return Response(url=req.url, status=200, headers={}, body=b"", request=req)

    engine.http.fetch = fetch  # type: ignore[method-assign]
    await engine.run()

    assert peak == {"a.test": 2, "b.test": 2}
    assert engine._domain_slots is not None
    assert engine._domain_slots.active_hosts() == 0  # no leaked host state


# -- #8 deduplication by request fingerprint ------------------------------------------


async def test_equivalent_urls_are_deduplicated() -> None:
    links = {
        "http://a.test/": [
            "http://a.test/p?a=1&b=2",
            "http://A.TEST:80/p?b=2&a=1#frag",
            "http://a.test/p?a=1&b=3",
        ]
    }
    engine, fetched = engine_for(LinkSpider(start_urls=["http://a.test/"]), links)
    result = await engine.run()

    assert len(fetched) == 3
    assert result.stats["dupe_filtered"] == 1


# -- #10 job persistence ------------------------------------------------------------------


class JobSpider(Spider):
    name = "job"
    start_urls = ("http://a.test/",)

    async def parse(self, response: Response) -> None:
        await self.emit({"url": response.url})
        if response.url == "http://a.test/":
            for i in range(10):
                await self.follow(f"http://a.test/{i}", callback=self.parse_page)

    async def parse_page(self, response: Response) -> None:
        await self.emit({"url": response.url})


async def test_job_dir_resumes_after_stop_without_refetching(tmp_path: Path) -> None:
    first, fetched_first = engine_for(
        JobSpider(), {}, concurrency=1, job_dir=tmp_path, max_requests=4
    )
    result = await first.run()
    assert result.close_reason == "max_requests"
    assert len(fetched_first) == 4

    second, fetched_second = engine_for(
        JobSpider(), {}, concurrency=1, job_dir=tmp_path
    )
    result = await second.run()

    assert result.close_reason == "finished"
    assert set(fetched_first).isdisjoint(fetched_second)
    assert sorted(fetched_first + fetched_second) == sorted(
        ["http://a.test/", *(f"http://a.test/{i}" for i in range(10))]
    )
    # A finished job starts fresh next time.
    third, fetched_third = engine_for(JobSpider(), {}, concurrency=1, job_dir=tmp_path)
    await third.run()
    assert len(fetched_third) == 11


async def test_job_dir_keeps_requests_of_interrupted_callbacks(tmp_path: Path) -> None:
    class Crashing(JobSpider):
        async def parse_page(self, response: Response) -> None:
            if response.url.endswith("/5"):
                engine.stop()
            await super().parse_page(response)

    engine, fetched = engine_for(Crashing(), {}, concurrency=1, job_dir=tmp_path)
    await engine.run()
    state = JobState(tmp_path, JobSpider())
    try:
        assert state.resumed
        restored = [request for _, request in state.load_pending()]
    finally:
        state.close(finished=False)
    assert {request.url for request in restored} == {
        f"http://a.test/{i}" for i in range(10)
    } - set(fetched)
    assert {getattr(r.callback, "__name__", None) for r in restored} == {"parse_page"}


async def test_job_dir_requires_method_callbacks(tmp_path: Path) -> None:
    async def standalone(response: Response) -> None:
        return None

    class BadCallbacks(Spider):
        name = "bad"
        start_urls = ("http://a.test/",)

        async def parse(self, response: Response) -> None:
            await self.follow("http://a.test/next", callback=standalone)

    engine, _ = engine_for(BadCallbacks(), {}, job_dir=tmp_path)
    await engine.run()
    assert engine.stats.labeled["errors_by_type"] == {"SpiderError": 1}


def test_job_state_round_trips_requests_and_rejects_other_spiders(
    tmp_path: Path,
) -> None:
    spider = JobSpider()
    state = JobState(tmp_path, spider)
    request = Request(
        url="http://a.test/x",
        method="POST",
        headers={"x": "1"},
        params={"q": "a", "tags": ["x", "y"]},
        data=b"\x00\xffbinary",
        meta={"depth": 2, "nested": {"a": [1, 2]}},
        timeout=2.5,
        callback=spider.parse_page,
        errback=None,
        dont_filter=True,
        priority=7,
    )
    state.add_pending(1, request, state.serialize(request))
    [(seq, restored)] = state.load_pending()
    state.close(finished=False)

    assert seq == 1
    assert restored.replace(callback=None) == request.replace(callback=None)
    assert restored.callback == spider.parse_page

    with pytest.raises(ValueError, match="belongs to spider 'job'"):
        JobState(tmp_path, Spider(name="other"))
    with pytest.raises(SpiderError, match="JSON-serializable"):
        JobState(tmp_path, spider).serialize(
            Request(url="http://a.test/", meta=cast("Any", {"x": object()}))
        )


# -- #13 metrics ----------------------------------------------------------------------------


async def test_metrics_endpoint_serves_prometheus_text() -> None:
    scraped: list[str] = []

    class Scraper(Spider):
        name = "metrics"
        start_urls = ("http://a.test/",)

        async def parse(self, response: Response) -> None:
            assert engine.metrics_server is not None
            port = engine.metrics_server.port
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
            await writer.drain()
            scraped.append((await reader.read()).decode())
            writer.close()
            self.stats_payload["pages"] = 1
            await self.emit({"ok": True})

    engine, _ = engine_for(Scraper(), {}, metrics_port=0)
    await engine.run()

    text = scraped[0]
    assert text.startswith("HTTP/1.1 200 OK")
    assert 'silkworm_requests_sent_total{spider="metrics"} 1' in text
    assert 'silkworm_responses_by_status_total{spider="metrics",status="200"} 1' in text
    assert 'silkworm_in_flight{spider="metrics"} 1' in text
    assert "# TYPE silkworm_queue_size gauge" in text
    final = engine.metrics_text()
    assert 'silkworm_items_scraped_total{spider="metrics"} 1' in final
    assert 'silkworm_custom_pages{spider="metrics"} 1' in final


def test_labeled_counters_cap_cardinality() -> None:
    stats = CrawlStats()
    for i in range(MAX_LABELS + 50):
        stats.inc_labeled("requests_by_domain", f"host{i}.test")
    counter = stats.labeled["requests_by_domain"]
    assert len(counter) == MAX_LABELS + 1
    assert counter[OTHER_LABEL] == 50
