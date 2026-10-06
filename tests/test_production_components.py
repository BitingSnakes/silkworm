"""Production components: retries, signals, throttling, robots.txt, URL
fingerprints, settings, CLI helpers, testing helpers, and item validation."""

from __future__ import annotations

import asyncio
import os
import signal
import sys
from pathlib import Path
from typing import Any

import pytest

from silkworm import (
    Engine,
    HttpConnectionError,
    HttpTimeoutError,
    IgnoreRequest,
    Request,
    Response,
    Spider,
    SpiderError,
    canonicalize_url,
    crawl,
    request_fingerprint,
)
from silkworm.cli import CliError, _split_reference, load_spider_class, output_pipeline
from silkworm.exceptions import CrawlFailedError, DropItem
from silkworm.middlewares import (
    AutoThrottleMiddleware,
    RetryMiddleware,
    RobotsTxtMiddleware,
)
from silkworm.pipelines import CSVPipeline, JsonLinesPipeline, ValidationPipeline
from silkworm.settings import coerce_setting, resolve_options
from silkworm.testing import (
    html_response,
    response_from_file,
    run_callback,
    run_errback,
    run_start_requests,
)


def ok(
    req: Request, status: int = 200, headers: dict[str, str] | None = None
) -> Response:
    return Response(
        url=req.url, status=status, headers=headers or {}, body=b"", request=req
    )


# -- #2 retrying transport failures ---------------------------------------------------


async def test_retry_middleware_retries_transient_exceptions_only() -> None:
    middleware = RetryMiddleware(max_times=2, backoff_base=0)
    request = Request(url="http://a.test/")
    spider = Spider()

    first = await middleware.process_exception(request, HttpTimeoutError("t"), spider)
    assert first is not None and first.meta["retry_times"] == 1 and first.dont_filter
    second = await middleware.process_exception(first, HttpConnectionError("c"), spider)
    assert second is not None and second.meta["retry_times"] == 2
    assert (
        await middleware.process_exception(second, HttpTimeoutError("t"), spider)
        is None
    )
    assert (
        await middleware.process_exception(request, SpiderError("bug"), spider) is None
    )
    assert await middleware.process_exception(request, ValueError("x"), spider) is None
    assert (
        await RetryMiddleware(retry_exceptions=()).process_exception(
            request, HttpTimeoutError("t"), spider
        )
        is None
    )


async def test_engine_retries_connection_errors_without_counting_errors() -> None:
    attempts: list[int] = []

    class OneShot(Spider):
        start_urls = ("http://a.test/",)

        async def parse(self, response: Response) -> None:
            await self.emit({"ok": True})

    engine = Engine(
        OneShot(), concurrency=1, response_middlewares=[RetryMiddleware(backoff_base=0)]
    )

    async def flaky(req: Request) -> Response:
        attempts.append(req.meta.get("retry_times", 0))  # type: ignore[arg-type]
        if len(attempts) < 3:
            raise HttpConnectionError("connection reset")
        return ok(req)

    engine.http.fetch = flaky  # type: ignore[method-assign]
    result = await engine.run()

    assert attempts == [0, 1, 2]
    assert (result.errors, result.stats["retries"], result.items_scraped) == (0, 2, 1)


# -- #5 graceful shutdown on signals ------------------------------------------------------


@pytest.mark.skipif(
    sys.platform == "win32", reason="loop signal handlers are POSIX-only"
)
async def test_sigint_stops_crawl_gracefully_and_second_forces_cancel() -> None:
    finished: list[str] = []

    class Interrupted(Spider):
        start_urls = tuple(f"http://a.test/{i}" for i in range(10))

        async def parse(self, response: Response) -> None:
            if response.url.endswith("/0"):
                os.kill(os.getpid(), signal.SIGINT)
                await asyncio.sleep(0.05)
            finished.append(response.url)

    async def fetch(req: Request) -> Response:
        return ok(req)

    class Client:
        concurrency = 1
        html_max_size_bytes = 1_000_000

        async def fetch(self, req: Request) -> Response:
            return await fetch(req)

        async def close(self) -> None:
            return None

    result = await crawl(Interrupted(), handle_signals=True, http_client=Client())
    assert result.close_reason == "shutdown"
    assert finished == ["http://a.test/0"]  # in-flight callback completed
    assert signal.getsignal(signal.SIGINT) is signal.default_int_handler

    class Stubborn(Spider):
        start_urls = ("http://a.test/",)

        async def parse(self, response: Response) -> None:
            os.kill(os.getpid(), signal.SIGINT)
            await asyncio.sleep(0.02)
            os.kill(os.getpid(), signal.SIGINT)
            await asyncio.sleep(5)

    with pytest.raises(KeyboardInterrupt):
        await crawl(Stubborn(), handle_signals=True, http_client=Client())


# -- #6 AutoThrottle -------------------------------------------------------------------------


async def test_autothrottle_spaces_requests_per_host() -> None:
    throttle = AutoThrottleMiddleware(start_delay=0.05, max_delay=1.0)
    spider = Spider()
    loop = asyncio.get_running_loop()
    released: list[tuple[str, float]] = []

    async def send(url: str) -> None:
        await throttle.process_request(Request(url=url), spider)
        released.append((url, loop.time()))

    start = loop.time()
    await asyncio.gather(
        send("http://a.test/1"), send("http://a.test/2"), send("http://b.test/1")
    )
    a_times = sorted(t for url, t in released if "a.test" in url)
    b_time = next(t for url, t in released if "b.test" in url)
    assert a_times[1] - a_times[0] >= 0.045
    assert b_time - start < 0.03  # other hosts are not delayed


async def test_autothrottle_adapts_to_latency_and_backs_off() -> None:
    throttle = AutoThrottleMiddleware(start_delay=1.0, min_delay=0.0, max_delay=8.0)
    spider = Spider()
    url = "http://a.test/"

    async def roundtrip(
        status: int, latency: float, headers: dict[str, str] | None = None
    ) -> None:
        request = await throttle.process_request(Request(url=url), spider)
        request.meta["_autothrottle_start"] -= latency  # type: ignore[operator]
        await throttle.process_response(ok(request, status, headers), spider)
        throttle._hosts["a.test"].next_request_at = 0.0  # skip real waiting

    await roundtrip(200, latency=0.2)
    assert throttle.delay_for(url) == pytest.approx(0.6, abs=0.05)  # (1.0 + 0.2) / 2
    await roundtrip(500, latency=0.0)
    assert throttle.delay_for(url) == pytest.approx(
        0.6, abs=0.05
    )  # errors never speed up
    await roundtrip(429, latency=0.0, headers={"retry-after": "3"})
    assert throttle.delay_for(url) == pytest.approx(1.2, abs=0.1)
    for _ in range(5):
        await roundtrip(503, latency=0.0)
    assert throttle.delay_for(url) == 8.0  # clamped to max_delay


def test_autothrottle_validates_bounds() -> None:
    with pytest.raises(ValueError, match="start_delay must be between"):
        AutoThrottleMiddleware(start_delay=10, max_delay=5)
    with pytest.raises(ValueError, match="target_concurrency must be positive"):
        AutoThrottleMiddleware(target_concurrency=0)


# -- #7 robots.txt ---------------------------------------------------------------------------


class _FetchError(Exception):
    def __init__(self, status: int | None) -> None:
        super().__init__(f"status {status}")
        self.status = status


def robots_fetcher(files: dict[str, str | int | None]):
    calls: list[str] = []

    async def fetch(url: str) -> str:
        calls.append(url)
        value = files.get(url, 404)
        if isinstance(value, str):
            return value
        raise _FetchError(value)

    return fetch, calls


async def test_robots_middleware_blocks_disallowed_paths_per_origin() -> None:
    fetch, calls = robots_fetcher(
        {
            "http://a.test/robots.txt": "User-agent: *\nDisallow: /private\n",
            "http://b.test/robots.txt": 404,
        }
    )
    middleware = RobotsTxtMiddleware(fetcher=fetch, obey_crawl_delay=False)
    spider = Spider()

    assert await middleware.process_request(Request(url="http://a.test/public"), spider)
    with pytest.raises(IgnoreRequest) as blocked:
        await middleware.process_request(Request(url="http://a.test/private/x"), spider)
    assert blocked.value.reason == "robots_txt"
    assert await middleware.process_request(
        Request(url="http://b.test/private"), spider
    )
    assert await middleware.process_request(
        Request(url="http://a.test/private", meta={"dont_obey_robotstxt": True}), spider
    )
    assert calls == ["http://a.test/robots.txt", "http://b.test/robots.txt"]


async def test_robots_middleware_unavailable_policy_and_user_agent_groups() -> None:
    fetch, _ = robots_fetcher(
        {
            "http://down.test/robots.txt": 503,
            "http://ua.test/robots.txt": "User-agent: silkbot\nDisallow: /\n\nUser-agent: *\nAllow: /\n",
        }
    )
    spider = Spider()
    allow = RobotsTxtMiddleware(fetcher=fetch)
    assert await allow.process_request(Request(url="http://down.test/x"), spider)
    deny = RobotsTxtMiddleware(fetcher=fetch, on_unavailable="disallow")
    with pytest.raises(IgnoreRequest):
        await deny.process_request(Request(url="http://down.test/x"), spider)

    bot = RobotsTxtMiddleware(fetcher=fetch, user_agent="silkbot")
    with pytest.raises(IgnoreRequest):
        await bot.process_request(Request(url="http://ua.test/x"), spider)
    assert await allow.process_request(Request(url="http://ua.test/x"), spider)


async def test_robots_middleware_applies_crawl_delay() -> None:
    fetch, _ = robots_fetcher(
        {"http://a.test/robots.txt": "User-agent: *\nCrawl-delay: 0.05\n"}
    )
    middleware = RobotsTxtMiddleware(fetcher=fetch)
    loop = asyncio.get_running_loop()
    times: list[float] = []
    for i in range(3):
        await middleware.process_request(Request(url=f"http://a.test/{i}"), Spider())
        times.append(loop.time())
    assert times[2] - times[0] >= 0.09


async def test_robots_blocked_requests_are_counted_by_engine() -> None:
    fetch, _ = robots_fetcher(
        {"http://a.test/robots.txt": "User-agent: *\nDisallow: /no\n"}
    )

    class Two(Spider):
        start_urls = ("http://a.test/yes", "http://a.test/no")

        async def parse(self, response: Response) -> None:
            return None

    engine = Engine(Two(), request_middlewares=[RobotsTxtMiddleware(fetcher=fetch)])

    async def fetch_page(req: Request) -> Response:
        return ok(req)

    engine.http.fetch = fetch_page  # type: ignore[method-assign]
    result = await engine.run()
    assert result.requests_sent == 1
    assert result.labeled_stats["ignored_by_reason"] == {"robots_txt": 1}


# -- #8 canonical URLs and fingerprints ---------------------------------------------------------


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("HTTP://Example.COM:80/a?b=2&a=1#top", "http://example.com/a?a=1&b=2"),
        ("https://example.com", "https://example.com/"),
        ("https://example.com:8443/p%7eq", "https://example.com:8443/p~q"),
        ("https://example.com/a%2Fb/c", "https://example.com/a%2Fb/c"),
        ("https://example.com/?q=a+b&empty=", "https://example.com/?empty=&q=a%20b"),
        ("http://[::1]:80/x", "http://[::1]/x"),
    ],
)
def test_canonicalize_url(url: str, expected: str) -> None:
    assert canonicalize_url(url) == expected


def test_canonicalize_url_can_keep_fragments() -> None:
    assert (
        canonicalize_url("https://x.test/a#b", keep_fragments=True)
        == "https://x.test/a#b"
    )


def test_request_fingerprint_covers_method_url_params_and_body() -> None:
    base = Request(url="https://x.test/s?a=1&b=2")
    assert request_fingerprint(base) == request_fingerprint(
        Request(url="https://X.test/s?b=2#x", params={"a": "1"}, headers={"h": "v"})
    )
    assert request_fingerprint(base) != request_fingerprint(base.replace(method="POST"))
    post = Request(url="https://x.test/api", method="POST", json={"a": 1, "b": 2})
    assert request_fingerprint(post) == request_fingerprint(
        post.replace(json={"b": 2, "a": 1})
    )
    assert request_fingerprint(post) != request_fingerprint(post.replace(json={"a": 2}))
    assert request_fingerprint(
        post.replace(json=None, data=b"x")
    ) != request_fingerprint(post.replace(json=None, data=b"y"))


# -- #12 settings and CLI helpers ------------------------------------------------------------


class SettingsSpider(Spider):
    name = "settings"
    custom_settings = {"concurrency": 4, "max_depth": 2, "unrelated": "kept"}  # noqa: RUF012


def test_resolve_options_layers_env_spider_and_explicit() -> None:
    env = {
        "SILKWORM_CONCURRENCY": "8",
        "SILKWORM_MAX_ITEMS": "100",
        "SILKWORM_KEEP_ALIVE": "yes",
        "SILKWORM_ITEM_BATCH_SIZE": "50",
        "SILKWORM_ITEM_BATCH_WAIT": "0.1",
    }
    options = resolve_options(SettingsSpider(), {"max_depth": 5}, environ=env)
    assert options == {
        "concurrency": 4,
        "max_items": 100,
        "keep_alive": True,
        "item_batch_size": 50,
        "item_batch_wait": 0.1,
        "max_depth": 5,
    }


def test_settings_validation_errors_name_the_source() -> None:
    with pytest.raises(ValueError, match="SILKWORM_MAX_DEPTH"):
        resolve_options(Spider(), {}, environ={"SILKWORM_MAX_DEPTH": "three"})
    with pytest.raises(KeyError, match="Unknown engine setting 'concurrencyy'"):
        coerce_setting("concurrencyy", "1")
    assert coerce_setting("max_duration", "none") is None
    assert coerce_setting("max_error_rate", "0.25") == 0.25
    with pytest.raises(ValueError, match="boolean"):
        coerce_setting("keep_alive", "maybe")


def test_cli_loads_spiders_from_files_and_references(tmp_path: Path) -> None:
    single = tmp_path / "single.py"
    single.write_text(
        "from silkworm import Spider\n"
        "from silkworm.spiders import Spider as Imported\n"
        "class Only(Spider):\n    name = 'only'\n"
    )
    assert load_spider_class(str(single)).__name__ == "Only"

    multi = tmp_path / "multi.py"
    multi.write_text(
        "from silkworm import Spider\n"
        "class A(Spider):\n    pass\n"
        "class B(Spider):\n    pass\n"
    )
    with pytest.raises(CliError, match=r"several spiders \(A, B\)"):
        load_spider_class(str(multi))
    assert load_spider_class(f"{multi}:B").__name__ == "B"
    with pytest.raises(CliError, match="not found"):
        load_spider_class(str(tmp_path / "missing.py"))
    assert _split_reference(r"C:\spiders\q.py") == (r"C:\spiders\q.py", None)
    assert _split_reference(r"C:\spiders\q.py:Q") == (r"C:\spiders\q.py", "Q")
    assert _split_reference("pkg.mod:Q") == ("pkg.mod", "Q")


def test_cli_output_pipeline_follows_extension(tmp_path: Path) -> None:
    assert isinstance(output_pipeline(str(tmp_path / "a.jl")), JsonLinesPipeline)
    assert isinstance(output_pipeline(str(tmp_path / "nested" / "a.csv")), CSVPipeline)
    assert (tmp_path / "nested").is_dir()
    with pytest.raises(CliError, match="Unsupported output format"):
        output_pipeline(str(tmp_path / "a.docx"))


# -- #14 testing helpers ----------------------------------------------------------------------------


class QuotesSpider(Spider):
    start_urls = ("https://q.test/",)

    async def parse(self, response: Response) -> None:
        await self.emit({"title": "t", "url": response.url})
        await self.follow("/page/2/")

    async def parse_detail(self, response: Response) -> None:
        await self.follow("https://q.test/other", callback=self.parse)

    async def handle_error(self, request: Request, exc: Exception) -> None:
        await self.emit({"failed": request.url, "error": type(exc).__name__})


async def test_run_callback_collects_items_and_resolved_requests() -> None:
    spider = QuotesSpider()
    response = html_response("<html></html>", "https://q.test/", callback=spider.parse)
    result = await run_callback(spider.parse, response)

    assert result.items == [{"title": "t", "url": "https://q.test/"}]
    assert result.urls == ["https://q.test/page/2/"]
    assert result.requests[0].callback == spider.parse  # inherited


async def test_run_start_requests_and_errback() -> None:
    spider = QuotesSpider()
    assert [r.url for r in await run_start_requests(spider)] == ["https://q.test/"]
    result = await run_errback(
        spider.handle_error, Request(url="https://q.test/x"), TimeoutError()
    )
    assert result.items == [{"failed": "https://q.test/x", "error": "TimeoutError"}]


async def test_run_callback_enforces_contract_and_propagates_errors() -> None:
    async def generator(response: Response):
        yield {"x": 1}

    async def failing(response: Response) -> None:
        raise ValueError("boom")

    response = html_response("<p></p>")
    with pytest.raises(SpiderError, match="async generator"):
        await run_callback(generator, response)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="boom"):
        await run_callback(failing, response)


def test_response_from_file_picks_response_type(tmp_path: Path) -> None:
    page = tmp_path / "page.html"
    page.write_text("<html><p>hi</p></html>")
    data = tmp_path / "data.json"
    data.write_text('{"a": 1}')
    html = response_from_file(page, "https://x.test/")
    assert type(html).__name__ == "HTMLResponse" and html.url == "https://x.test/"
    json_response = response_from_file(data)
    assert type(json_response).__name__ == "Response"
    assert json_response.headers["content-type"] == "application/json"


# -- #15 item validation --------------------------------------------------------------------------


def _quote_model() -> type[Any]:
    pydantic = pytest.importorskip("pydantic")

    class Quote(pydantic.BaseModel):
        text: str
        author: str
        tags: list[str] = pydantic.Field(default_factory=list)

    return Quote


async def test_validation_pipeline_with_model_normalizes_and_drops() -> None:
    pipeline = ValidationPipeline(_quote_model(), log_limit=1)
    spider = Spider()
    await pipeline.open(spider)
    assert await pipeline.process_item({"text": "a", "author": "b"}, spider) == {
        "text": "a",
        "author": "b",
        "tags": [],
    }
    with pytest.raises(DropItem) as dropped:
        await pipeline.process_item({"text": "a"}, spider)
    assert dropped.value.reason == "invalid"
    assert (pipeline.valid, pipeline.invalid) == (1, 1)


async def test_validation_pipeline_with_callable_and_raise_mode() -> None:
    def require_price(item: Any) -> Any:
        try:
            price = float(item["price"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("price must be a number") from exc
        return {**item, "price": round(price, 2)}

    spider = Spider()
    assert await ValidationPipeline(require_price).process_item(
        {"price": 1.234}, spider
    ) == {"price": 1.23}
    with pytest.raises(ValueError, match="price must be a number"):
        await ValidationPipeline(require_price, on_invalid="raise").process_item(
            {}, spider
        )
    with pytest.raises(TypeError, match="model class"):
        ValidationPipeline("not a schema")  # type: ignore[arg-type]


async def test_engine_counts_invalid_items_and_fails_on_drop_rate() -> None:
    class Redesigned(Spider):
        start_urls = ("http://a.test/",)

        async def parse(self, response: Response) -> None:
            await self.emit({"text": "ok", "author": "a"})
            for _ in range(3):
                await self.emit({"text": "author selector broke"})

    engine = Engine(
        Redesigned(),
        item_pipelines=[ValidationPipeline(_quote_model())],
        max_item_drop_rate=0.5,
    )

    async def fetch(req: Request) -> Response:
        return ok(req)

    engine.http.fetch = fetch  # type: ignore[method-assign]
    with pytest.raises(CrawlFailedError, match="item drop rate 75.0%") as caught:
        await engine.run()
    result = caught.value.result
    assert (result.items_scraped, result.items_dropped) == (1, 3)
    assert result.labeled_stats["items_dropped_by_reason"] == {"invalid": 3}
