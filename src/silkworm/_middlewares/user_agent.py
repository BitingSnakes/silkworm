from __future__ import annotations

import random
from typing import TYPE_CHECKING

from ..logging import Logger, get_logger
from ..request import Request

if TYPE_CHECKING:
    from collections.abc import Sequence

    from ..spiders import Spider


class UserAgentMiddleware:
    """Set a missing ``User-Agent`` header from a pool or fixed default.

    Args:
        user_agents: Values sampled independently for each request.
        default: Value used when the pool is empty. Defaults to
            ``"silkworm/0.1"``.

    Existing request headers are never overwritten.
    """

    def __init__(
        self,
        user_agents: Sequence[str] | None = None,
        *,
        default: str | None = None,
    ) -> None:
        self.user_agents: list[str] = list(user_agents or [])
        self.default: str = default or "silkworm/0.1"
        self.logger: Logger = get_logger(component="UserAgentMiddleware")

    async def process_request(self, request: Request, spider: Spider) -> Request:
        """Set a user agent if the request does not already define one."""
        ua = None
        if self.user_agents:
            ua = random.choice(self.user_agents)
        else:
            ua = self.default
        request.headers.setdefault("User-Agent", ua)
        self.logger.debug("Assigned user agent", user_agent=ua, url=request.url)
        return request
