from __future__ import annotations

import asyncio
import random
from collections.abc import Callable
from enum import Enum, auto
from typing import TYPE_CHECKING, assert_never

from ..logging import get_logger
from ..request import Request

if TYPE_CHECKING:
    from ..spiders import Spider


class _DelayStrategy(Enum):
    """Internal enum to track which delay strategy is configured."""

    FIXED = auto()
    RANDOM = auto()
    CUSTOM = auto()


class DelayMiddleware:
    """
    Middleware to add configurable delays between requests.

    Supports three delay strategies:
    1. Fixed delay: Always wait the same amount of time
    2. Random delay: Wait a random time between min and max
    3. Custom delay: Use a callable that returns delay duration

    Args:
        delay: Fixed delay in seconds, or None if using delay_func
        min_delay: Minimum delay for random strategy (requires max_delay)
        max_delay: Maximum delay for random strategy (requires min_delay)
        delay_func: Custom callable that returns delay in seconds.
                   Called with (request, spider) and should return float.

    Examples:
        Fixed delay of 1 second:
            DelayMiddleware(delay=1.0)

        Random delay between 0.5 and 2 seconds:
            DelayMiddleware(min_delay=0.5, max_delay=2.0)

        Custom delay function:
            def my_delay(request, spider):
                return 1.0 if "fast" in request.url else 2.0
            DelayMiddleware(delay_func=my_delay)
    """

    def __init__(
        self,
        delay: float | None = None,
        min_delay: float | None = None,
        max_delay: float | None = None,
        delay_func: Callable[[Request, Spider], float] | None = None,
    ) -> None:
        # Validate configuration and determine strategy
        self._delay_func: Callable[[Request, Spider], float] | None = None
        self._min_delay: float | None = None
        self._max_delay: float | None = None
        self._fixed_delay: float | None = None

        if delay_func is not None:
            if delay is not None or min_delay is not None or max_delay is not None:
                msg = "delay_func cannot be used with delay, min_delay, or max_delay"
                raise ValueError(msg)
            self._strategy = _DelayStrategy.CUSTOM
            self._delay_func = delay_func
        elif min_delay is not None or max_delay is not None:
            if delay is not None:
                msg = "Cannot use both delay and min_delay/max_delay"
                raise ValueError(msg)
            if min_delay is None or max_delay is None:
                msg = "Both min_delay and max_delay must be provided"
                raise ValueError(msg)
            if min_delay < 0 or max_delay < 0:
                msg = "min_delay and max_delay must be non-negative"
                raise ValueError(msg)
            if min_delay > max_delay:
                msg = "min_delay must be less than or equal to max_delay"
                raise ValueError(msg)
            self._strategy = _DelayStrategy.RANDOM
            self._min_delay = min_delay
            self._max_delay = max_delay
        elif delay is not None:
            if delay < 0:
                msg = "delay must be non-negative"
                raise ValueError(msg)
            self._strategy = _DelayStrategy.FIXED
            self._fixed_delay = delay
        else:
            msg = "Must provide one of: delay, min_delay/max_delay, or delay_func"
            raise ValueError(msg)

        self.logger = get_logger(component="DelayMiddleware")

    async def process_request(self, request: Request, spider: Spider) -> Request:
        """Calculate and apply delay before processing the request."""
        match self._strategy:
            case _DelayStrategy.CUSTOM:
                assert self._delay_func is not None
                delay = self._delay_func(request, spider)
            case _DelayStrategy.RANDOM:
                assert self._min_delay is not None and self._max_delay is not None
                delay = random.uniform(self._min_delay, self._max_delay)
            case _DelayStrategy.FIXED:
                assert self._fixed_delay is not None
                delay = self._fixed_delay
            case other:
                assert_never(other)

        if delay > 0:
            self.logger.debug(
                "Delaying request",
                url=request.url,
                delay=round(delay, 3),
            )
            await asyncio.sleep(delay)

        return request
