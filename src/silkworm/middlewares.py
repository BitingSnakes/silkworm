"""Request/response middlewares.

Implementations live in :mod:`silkworm._middlewares`; this module re-exports
the public API.
"""

from ._middlewares.autothrottle import AutoThrottleMiddleware
from ._middlewares.base import (
    ExceptionMiddleware,
    RequestMiddleware,
    ResponseMiddleware,
)
from ._middlewares.cloudflare import CloudflareCrawlMiddleware
from ._middlewares.cookies import CookiesMiddleware
from ._middlewares.delay import DelayMiddleware
from ._middlewares.proxy import ProxyMiddleware
from ._middlewares.retry import DEFAULT_RETRY_EXCEPTIONS, RetryMiddleware
from ._middlewares.robots import (
    RobotsOrigin,
    RobotsTxtDelayMiddleware,
    RobotsTxtFetcher,
    RobotsTxtMiddleware,
)
from ._middlewares.skip_non_html import SkipNonHTMLMiddleware
from ._middlewares.stream import RequestResponseStreamMiddleware
from ._middlewares.user_agent import UserAgentMiddleware

__all__ = [
    "DEFAULT_RETRY_EXCEPTIONS",
    "AutoThrottleMiddleware",
    "CloudflareCrawlMiddleware",
    "CookiesMiddleware",
    "DelayMiddleware",
    "ExceptionMiddleware",
    "ProxyMiddleware",
    "RequestMiddleware",
    "RequestResponseStreamMiddleware",
    "ResponseMiddleware",
    "RetryMiddleware",
    "RobotsOrigin",
    "RobotsTxtDelayMiddleware",
    "RobotsTxtFetcher",
    "RobotsTxtMiddleware",
    "SkipNonHTMLMiddleware",
    "UserAgentMiddleware",
]
