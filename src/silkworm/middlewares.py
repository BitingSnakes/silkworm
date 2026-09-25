"""Request/response middlewares.

Implementations live in :mod:`silkworm._middlewares`; this module re-exports
the public API.
"""

from ._middlewares.base import (
    RequestMiddleware,
    ResponseMiddleware,
    ExceptionMiddleware,
)
from ._middlewares.stream import RequestResponseStreamMiddleware
from ._middlewares.user_agent import UserAgentMiddleware
from ._middlewares.proxy import ProxyMiddleware
from ._middlewares.cookies import CookiesMiddleware
from ._middlewares.retry import RetryMiddleware
from ._middlewares.robots import (
    RobotsTxtFetcher,
    RobotsOrigin,
    RobotsTxtDelayMiddleware,
)
from ._middlewares.delay import DelayMiddleware
from ._middlewares.skip_non_html import SkipNonHTMLMiddleware
from ._middlewares.cloudflare import CloudflareCrawlMiddleware

__all__ = [
    "RequestMiddleware",
    "ResponseMiddleware",
    "ExceptionMiddleware",
    "RequestResponseStreamMiddleware",
    "UserAgentMiddleware",
    "ProxyMiddleware",
    "CookiesMiddleware",
    "RetryMiddleware",
    "RobotsTxtFetcher",
    "RobotsOrigin",
    "RobotsTxtDelayMiddleware",
    "DelayMiddleware",
    "SkipNonHTMLMiddleware",
    "CloudflareCrawlMiddleware",
]
