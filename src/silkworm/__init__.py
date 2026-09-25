from __future__ import annotations

from .api import fetch_html, fetch_html_cdp, fetch_html_servo
from .engine import DedupKey, Engine, EngineLogger, default_dedup_key
from .exceptions import (
    HttpError,
    MarkdownConversionError,
    SelectorError,
    SilkwormError,
    SpiderError,
)
from .logging import get_logger
from .markdown import (
    MarkdownStream,
    convert_html_to_markdown,
    html_to_markdown,
    stream_html_to_markdown,
    stream_html_to_markdown_async,
)
from .middlewares import (
    CookiesMiddleware,
    RequestResponseStreamMiddleware,
    RobotsTxtDelayMiddleware,
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
    "CookiesMiddleware",
    "DedupKey",
    "Engine",
    "EngineLogger",
    "HTMLResponse",
    "HttpError",
    "MarkdownConversionError",
    "MarkdownStream",
    "OnionLinkClient",
    "Request",
    "RequestResponseStreamMiddleware",
    "Response",
    "RobotsTxtDelayMiddleware",
    "SelectorError",
    "ServoFetchClient",
    "SilkwormError",
    "Spider",
    "SpiderError",
    "convert_html_to_markdown",
    "crawl",
    "default_dedup_key",
    "fetch_html",
    "fetch_html_cdp",
    "fetch_html_servo",
    "get_logger",
    "html_to_markdown",
    "run_spider",
    "run_spider_rsloop",
    "run_spider_trio",
    "run_spider_uvloop",
    "run_spider_winloop",
    "stream_html_to_markdown",
    "stream_html_to_markdown_async",
]

# Optional CDP support
try:
    from .cdp import CDPClient  # noqa: F401

    __all__.append("CDPClient")
except ImportError:
    pass
