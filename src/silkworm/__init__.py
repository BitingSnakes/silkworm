from __future__ import annotations

from .request import Request
from .response import Response, HTMLResponse
from .spiders import Spider
from .exceptions import SilkwormError, HttpError, SpiderError, SelectorError
from .engine import Engine
from .runner import (
    crawl,
    run_spider,
    run_spider_uvloop,
    run_spider_winloop,
    run_spider_trio,
)
from .api import fetch_html, fetch_html_cdp
from .logging import get_logger

__all__ = [
    "Request",
    "Response",
    "HTMLResponse",
    "SilkwormError",
    "HttpError",
    "SpiderError",
    "Spider",
    "SelectorError",
    "Engine",
    "crawl",
    "run_spider",
    "run_spider_uvloop",
    "run_spider_winloop",
    "run_spider_trio",
    "fetch_html",
    "fetch_html_cdp",
    "get_logger",
]

# Optional CDP support
try:
    from .cdp import CDPClient  # noqa: F401

    __all__.append("CDPClient")
except ImportError:
    pass
