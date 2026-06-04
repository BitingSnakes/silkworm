from __future__ import annotations

from .request import Request
from .response import Response, HTMLResponse
from .spiders import Spider
from .exceptions import (
    SilkwormError,
    HttpError,
    SpiderError,
    SelectorError,
    MarkdownConversionError,
)
from .engine import DedupKey, Engine, EngineLogger, default_dedup_key
from .markdown import (
    MarkdownStream,
    convert_html_to_markdown,
    html_to_markdown,
    stream_html_to_markdown,
    stream_html_to_markdown_async,
)
from .onionlink import OnionLinkClient
from .runner import (
    crawl,
    run_spider,
    run_spider_rsloop,
    run_spider_uvloop,
    run_spider_winloop,
    run_spider_trio,
)
from .api import fetch_html, fetch_html_cdp, fetch_html_servo
from .logging import get_logger
from .middlewares import (
    CookiesMiddleware,
    RequestResponseStreamMiddleware,
    RobotsTxtDelayMiddleware,
)
from .servo import ServoFetchClient

__all__ = [
    "Request",
    "Response",
    "HTMLResponse",
    "SilkwormError",
    "HttpError",
    "SpiderError",
    "Spider",
    "SelectorError",
    "MarkdownConversionError",
    "Engine",
    "EngineLogger",
    "DedupKey",
    "default_dedup_key",
    "MarkdownStream",
    "convert_html_to_markdown",
    "html_to_markdown",
    "stream_html_to_markdown",
    "stream_html_to_markdown_async",
    "OnionLinkClient",
    "crawl",
    "run_spider",
    "run_spider_rsloop",
    "run_spider_uvloop",
    "run_spider_winloop",
    "run_spider_trio",
    "fetch_html",
    "fetch_html_cdp",
    "fetch_html_servo",
    "get_logger",
    "CookiesMiddleware",
    "RequestResponseStreamMiddleware",
    "RobotsTxtDelayMiddleware",
    "ServoFetchClient",
]

# Optional CDP support
try:
    from .cdp import CDPClient  # noqa: F401

    __all__.append("CDPClient")
except ImportError:
    pass
