"""Production features against a real local HTTP server and the real wreq client:
body fidelity and size limits, transport error classification with retries,
the on-disk HTTP cache, and the command line."""

from __future__ import annotations

import asyncio
import json
import socket
import threading
import time
from collections import Counter
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

from silkworm import (
    HTMLResponse,
    HttpCache,
    HttpConnectionError,
    HttpTimeoutError,
    Request,
    Response,
    ResponseTooLargeError,
    Spider,
)
from silkworm.cli import main
from silkworm.http import HttpClient
from silkworm.httpcache import CACHE_HEADER
from silkworm.middlewares import (
    RetryMiddleware,
    RobotsTxtDelayMiddleware,
    RobotsTxtMiddleware,
)
from silkworm.runner import crawl

BINARY = bytes(range(256)) * 16
LATIN1_PAGE = (
    "<html><head><meta charset='iso-8859-1'><title>café</title></head>"
    "<body><p class='q'>naïve &amp; café</p></body></html>"
).encode("latin-1")


def _pages(port: int) -> dict[str, tuple[int, str, bytes]]:
    base = f"http://127.0.0.1:{port}"
    index = (
        "<html><body>"
        + "".join(f"<a href='/item/{i}'>item {i}</a>" for i in range(5))
        + "</body></html>"
    )
    return {
        "/": (200, "text/html", index.encode()),
        **{
            f"/item/{i}": (200, "text/html", f"<html><h1>Item {i}</h1></html>".encode())
            for i in range(5)
        },
        "/binary": (200, "application/octet-stream", BINARY),
        "/latin1": (200, "text/html", LATIN1_PAGE),
        "/error": (503, "text/plain", b"try later"),
        "/robots.txt": (200, "text/plain", b"User-agent: *\nDisallow: /item/4\n"),
        "/base": (200, "text/plain", base.encode()),
    }


class LocalSite:
    """A threaded HTTP server with a hit counter and a few misbehaving routes."""

    def __init__(self) -> None:
        self.hits: Counter[str] = Counter()
        site = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self) -> None:
                path = self.path.split("?")[0]
                site.hits[path] += 1
                if path == "/slow":
                    time.sleep(1.0)
                if path == "/drop" and site.hits[path] < 3:
                    # No response: really drop the TCP connection (close() alone
                    # keeps the socket open while rfile/wfile reference it).
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.close_connection = True
                    return
                if path == "/chunked":
                    self.send_response(200)
                    self.send_header("content-type", "application/octet-stream")
                    self.send_header("transfer-encoding", "chunked")
                    self.end_headers()
                    for _ in range(8):
                        chunk = b"x" * 4096
                        self.wfile.write(
                            f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n"
                        )
                    self.wfile.write(b"0\r\n\r\n")
                    return
                status, content_type, body = site.pages.get(
                    path, (404, "text/plain", b"missing")
                )
                if path in {"/slow", "/drop"}:
                    status, content_type, body = 200, "text/plain", b"ok"
                self.send_response(status)
                self.send_header("content-type", content_type)
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: Any) -> None:
                return None

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        self.base = f"http://127.0.0.1:{self.port}"
        self.pages = _pages(self.port)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def url(self, path: str) -> str:
        return f"{self.base}{path}"


@pytest.fixture
def local_site() -> Iterator[LocalSite]:
    site = LocalSite()
    site.thread.start()
    try:
        yield site
    finally:
        site.server.shutdown()
        site.server.server_close()


# -- #9 body fidelity and size limits -----------------------------------------------------


async def test_http_client_returns_exact_body_bytes(local_site: LocalSite) -> None:
    async with HttpClient(emulation=None) as client:
        binary = await client.fetch(Request(local_site.url("/binary")))
        latin1 = await client.fetch(Request(local_site.url("/latin1")))

    assert binary.body == BINARY
    assert latin1.body == LATIN1_PAGE
    assert latin1.encoding == "iso-8859-1"
    assert "naïve & café" in latin1.text or "naïve &amp; café" in latin1.text


async def test_http_client_enforces_response_size_limit(local_site: LocalSite) -> None:
    async with HttpClient(emulation=None, max_response_size_bytes=1000) as client:
        with pytest.raises(ResponseTooLargeError, match="declares 4096 bytes"):
            await client.fetch(Request(local_site.url("/binary")))
        with pytest.raises(ResponseTooLargeError, match="exceeded the 1000-byte limit"):
            await client.fetch(Request(local_site.url("/chunked")))
        unlimited = await client.fetch(
            Request(local_site.url("/chunked"), meta={"max_response_size": None})
        )
        assert len(unlimited.body) == 8 * 4096
        with pytest.raises(ValueError, match="positive integer or None"):
            await client.fetch(
                Request(local_site.url("/"), meta={"max_response_size": 0})
            )


# -- #2 transport error classification and retries ------------------------------------------


async def test_transport_errors_are_classified(local_site: LocalSite) -> None:
    async with HttpClient(emulation=None, timeout=0.2) as client:
        with pytest.raises(HttpTimeoutError):
            await client.fetch(Request(local_site.url("/slow")))
        with pytest.raises(HttpConnectionError):
            await client.fetch(Request("http://127.0.0.1:1/unreachable"))


async def test_engine_retries_dropped_connections(local_site: LocalSite) -> None:
    seen: list[str] = []

    class Flaky(Spider):
        start_urls = (local_site.url("/drop"),)

        async def parse(self, response: Response) -> None:
            seen.append(response.text)

    result = await crawl(
        Flaky(),
        concurrency=1,
        response_middlewares=[RetryMiddleware(backoff_base=0)],
    )
    assert seen == ["ok"]
    assert local_site.hits["/drop"] == 3
    assert (result.errors, result.stats["retries"]) == (0, 2)


# -- #11 HTTP cache ------------------------------------------------------------------------------


class ItemsSpider(Spider):
    name = "items"

    def __init__(self, base: str, **kwargs: object) -> None:
        super().__init__(start_urls=[f"{base}/"], **kwargs)  # type: ignore[arg-type]
        self.cache_headers: list[str | None] = []

    async def parse(self, response: Response) -> None:
        self.cache_headers.append(response.headers.get(CACHE_HEADER))
        if not isinstance(response, HTMLResponse):
            return
        for link in await response.select("a"):
            if href := link.attr("href"):
                await response.follow(href, callback=self.parse_item)

    async def parse_item(self, response: Response) -> None:
        self.cache_headers.append(response.headers.get(CACHE_HEADER))
        if not isinstance(response, HTMLResponse):
            return
        title = await response.select_first("h1")
        await self.emit({"title": title.text if title else None})


async def test_http_cache_serves_second_run_from_disk(
    local_site: LocalSite, tmp_path: Path
) -> None:
    cache_dir = tmp_path / "cache"
    first = ItemsSpider(local_site.base)
    result = await crawl(first, http_cache=HttpCache(cache_dir))
    assert result.items_scraped == 5
    assert sum(local_site.hits.values()) == 6
    assert set(first.cache_headers) == {None}

    second = ItemsSpider(local_site.base)
    result = await crawl(second, http_cache=HttpCache(cache_dir))
    assert result.items_scraped == 5
    assert sum(local_site.hits.values()) == 6  # nothing downloaded again
    assert second.cache_headers == ["hit"] * 6


async def test_http_cache_expiration_statuses_and_opt_out(
    local_site: LocalSite, tmp_path: Path
) -> None:
    cache = HttpCache(tmp_path, expiration=0.2)
    async with HttpClient(emulation=None) as raw:
        client = cache.wrap(raw)
        await client.fetch(Request(local_site.url("/item/1")))
        cached = await client.fetch(Request(local_site.url("/item/1")))
        assert cached.headers[CACHE_HEADER] == "hit"
        await asyncio.sleep(0.25)
        fresh = await client.fetch(Request(local_site.url("/item/1")))
        assert CACHE_HEADER not in fresh.headers

        await client.fetch(Request(local_site.url("/error")))
        await client.fetch(Request(local_site.url("/error")))
        assert local_site.hits["/error"] == 2  # 503 is never cached

        opt_out = Request(local_site.url("/item/2"), meta={"dont_cache": True})
        await client.fetch(opt_out)
        await client.fetch(opt_out)
        assert local_site.hits["/item/2"] == 2
    assert (cache.hits, cache.stored) == (1, 2)


# -- #12 command line ------------------------------------------------------------------------------


def _write_spider(tmp_path: Path, base: str) -> Path:
    spider_file = tmp_path / "local_spider.py"
    spider_file.write_text(
        f"""
from silkworm import HTMLResponse, Response, Spider


class LocalSpider(Spider):
    name = "local"
    start_urls = ("{base}/",)

    def __init__(self, label: str = "none", **kwargs):
        super().__init__(**kwargs)
        self.label = label

    async def parse(self, response: Response) -> None:
        if not isinstance(response, HTMLResponse):
            return
        for link in await response.select("a"):
            if href := link.attr("href"):
                await response.follow(href, callback=self.parse_item)

    async def parse_item(self, response: Response) -> None:
        title = await response.select_first("h1")
        await self.emit({{"title": title.text if title else None, "label": self.label}})
"""
    )
    return spider_file


def _run_cli(
    capsys: pytest.CaptureFixture[str], args: list[str]
) -> tuple[int, str, str]:
    code = main(args)
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def test_cli_crawl_writes_outputs_and_applies_settings(
    local_site: LocalSite, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    spider_file = _write_spider(tmp_path, local_site.base)
    out = tmp_path / "out" / "items.jl"
    code, stdout, stderr = _run_cli(
        capsys,
        [
            "--log-level",
            "ERROR",
            "crawl",
            str(spider_file),
            "-o",
            str(out),
            "-o",
            "-",
            "-s",
            "max_items=3",
            "-s",
            "concurrency=1",
            "-a",
            "label=cli",
        ],
    )
    assert code == 0, stderr
    lines = [json.loads(line) for line in out.read_text().splitlines()]
    assert len(lines) == 3 and {item["label"] for item in lines} == {"cli"}
    assert [json.loads(line) for line in stdout.splitlines()] == lines
    assert "finished (max_items)" in stderr


def test_cli_crawl_exit_codes(
    local_site: LocalSite, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    spider_file = _write_spider(tmp_path, local_site.base)
    code, _, stderr = _run_cli(
        capsys,
        ["--log-level", "ERROR", "crawl", str(spider_file), "-s", "min_items=50"],
    )
    assert code == 1
    assert "crawl failed: scraped 5 items, fewer than min_items=50" in stderr

    code, _, stderr = _run_cli(
        capsys, ["crawl", str(spider_file), "-s", "max_depth=deep"]
    )
    assert code == 2 and "Invalid value 'deep'" in stderr
    code, _, stderr = _run_cli(capsys, ["crawl", str(spider_file), "-a", "nope=1"])
    assert code == 2 and "Cannot create LocalSpider" in stderr
    code, _, stderr = _run_cli(capsys, ["crawl", str(tmp_path / "missing.py")])
    assert code == 2 and "Spider file not found" in stderr


def test_cli_job_dir_and_http_cache_flags(
    local_site: LocalSite, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    spider_file = _write_spider(tmp_path, local_site.base)
    job_dir, cache_dir = tmp_path / "job", tmp_path / "cache"
    common = ["--log-level", "ERROR", "crawl", str(spider_file), "-s", "concurrency=1"]
    code, _, _ = _run_cli(
        capsys,
        [
            *common,
            "--job-dir",
            str(job_dir),
            "--http-cache",
            str(cache_dir),
            "-s",
            "max_requests=3",
        ],
    )
    assert code == 0
    fetched_first = sum(local_site.hits.values())
    assert fetched_first == 3
    code, _, _ = _run_cli(
        capsys, [*common, "--job-dir", str(job_dir), "-o", str(tmp_path / "rest.jl")]
    )
    assert code == 0
    assert sum(local_site.hits.values()) == 6  # resumed: only the 3 remaining pages
    assert len((tmp_path / "rest.jl").read_text().splitlines()) == 3


def test_cli_parse_and_fetch(
    local_site: LocalSite, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    spider_file = _write_spider(tmp_path, local_site.base)
    code, stdout, stderr = _run_cli(
        capsys, ["parse", local_site.url("/"), "--spider", str(spider_file)]
    )
    assert code == 0, stderr
    records = [json.loads(line) for line in stdout.splitlines()]
    assert [r["url"] for r in records] == [
        local_site.url(f"/item/{i}") for i in range(5)
    ]
    assert {r["callback"] for r in records} == {"parse_item"}

    code, stdout, _ = _run_cli(
        capsys,
        [
            "parse",
            local_site.url("/item/3"),
            "--spider",
            str(spider_file),
            "-c",
            "parse_item",
            "-a",
            "label=x",
        ],
    )
    assert code == 0
    assert json.loads(stdout) == {
        "type": "item",
        "item": {"title": "Item 3", "label": "x"},
    }

    target = tmp_path / "body.bin"
    code, _, stderr = _run_cli(
        capsys, ["fetch", local_site.url("/binary"), "-o", str(target), "--headers"]
    )
    assert code == 0 and target.read_bytes() == BINARY
    assert "content-type: application/octet-stream" in stderr


# -- #7 robots.txt over real HTTP ------------------------------------------------------------


async def test_robots_middleware_fetches_real_robots_txt(local_site: LocalSite) -> None:
    spider = ItemsSpider(local_site.base)
    result = await crawl(spider, request_middlewares=[RobotsTxtMiddleware()])

    assert result.items_scraped == 4  # /item/4 is disallowed
    assert result.labeled_stats["ignored_by_reason"] == {"robots_txt": 1}
    assert local_site.hits["/robots.txt"] == 1
    assert local_site.hits["/item/4"] == 0


async def test_robots_middlewares_treat_missing_robots_txt_as_unrestricted(
    local_site: LocalSite,
) -> None:
    # A 404 whose body looks like a restrictive robots.txt: it must be ignored.
    local_site.pages["/robots.txt"] = (
        404,
        "text/plain",
        b"User-agent: *\nCrawl-delay: 7\nDisallow: /\n",
    )
    result = await crawl(
        ItemsSpider(local_site.base), request_middlewares=[RobotsTxtMiddleware()]
    )
    assert result.items_scraped == 5
    assert "ignored_by_reason" not in {k for k, v in result.labeled_stats.items() if v}

    delay = RobotsTxtDelayMiddleware(local_site.base, fallback_delay=0.0)
    await delay.open(Spider())
    # The 404 body must not be parsed as robots.txt; the fallback applies.
    assert delay._delay_source == "fallback"
