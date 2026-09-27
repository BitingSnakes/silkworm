"""Public entry point for building and running asynchronous web spiders.

The package root exposes the core request, response, spider, engine, runner,
browser-backed client, middleware, logging, and HTML-to-Markdown APIs most
applications need. Specialized middleware and pipeline implementations are
available from :mod:`silkworm.middlewares` and :mod:`silkworm.pipelines`.
"""

from __future__ import annotations

from ._stats import CrawlResult
from ._urls import canonicalize_url, request_fingerprint
from .api import fetch_html, fetch_html_cdp, fetch_html_servo
from .cdp import CDPClient
from .engine import DedupKey, Engine, EngineLogger, EngineOptions, default_dedup_key
from .exceptions import (
    CloseSpider,
    CrawlFailedError,
    DropItem,
    HttpConnectionError,
    HttpError,
    HttpTimeoutError,
    IgnoreRequest,
    MarkdownConversionError,
    ResponseTooLargeError,
    SelectorError,
    SilkwormError,
    SpiderError,
)
from .httpcache import HttpCache
from .logging import get_logger
from .markdown import (
    MarkdownStream,
    convert_html_to_markdown,
    html_to_markdown,
    stream_html_to_markdown,
    stream_html_to_markdown_async,
)
from .middlewares import (
    AutoThrottleMiddleware,
    CookiesMiddleware,
    RequestResponseStreamMiddleware,
    RetryMiddleware,
    RobotsTxtDelayMiddleware,
    RobotsTxtMiddleware,
)
from .onionlink import OnionLinkClient
from .request import Request
from .response import HTMLResponse, Response
from .runner import (
    crawl,
    run_spider,
    run_spider_rsloop,
    run_spider_trio,
    run_spider_uvloop,
    run_spider_winloop,
)
from .servo import ServoFetchClient
from .spiders import Spider

__all__ = [
    "AutoThrottleMiddleware",
    "CDPClient",
    "CloseSpider",
    "CookiesMiddleware",
    "CrawlFailedError",
    "CrawlResult",
    "DedupKey",
    "DropItem",
    "Engine",
    "EngineLogger",
    "EngineOptions",
    "HTMLResponse",
    "HttpCache",
    "HttpConnectionError",
    "HttpError",
    "HttpTimeoutError",
    "IgnoreRequest",
    "MarkdownConversionError",
    "MarkdownStream",
    "OnionLinkClient",
    "Request",
    "RequestResponseStreamMiddleware",
    "Response",
    "ResponseTooLargeError",
    "RetryMiddleware",
    "RobotsTxtDelayMiddleware",
    "RobotsTxtMiddleware",
    "SelectorError",
    "ServoFetchClient",
    "SilkwormError",
    "Spider",
    "SpiderError",
    "canonicalize_url",
    "convert_html_to_markdown",
    "crawl",
    "default_dedup_key",
    "fetch_html",
    "fetch_html_cdp",
    "fetch_html_servo",
    "get_logger",
    "html_to_markdown",
    "request_fingerprint",
    "run_spider",
    "run_spider_rsloop",
    "run_spider_trio",
    "run_spider_uvloop",
    "run_spider_winloop",
    "stream_html_to_markdown",
    "stream_html_to_markdown_async",
]
