from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from ..logging import get_logger
from ..request import Request
from ..response import Response

if TYPE_CHECKING:
    from collections.abc import Iterable
    from ..spiders import Spider


class RetryMiddleware:
    def __init__(
        self,
        max_times: int = 3,
        retry_http_codes: Iterable[int] | None = None,
        backoff_base: float = 0.5,
        sleep_http_codes: Iterable[int] | None = None,
    ) -> None:
        if max_times < 0:
            msg = "max_times must be non-negative"
            raise ValueError(msg)
        if backoff_base < 0:
            msg = "backoff_base must be non-negative"
            raise ValueError(msg)

        self.max_times = max_times
        base_retry_codes = (
            set(retry_http_codes)
            if retry_http_codes is not None
            else {500, 502, 503, 504, 522, 524, 408, 429}
        )
        sleep_codes = (
            set(sleep_http_codes)
            if sleep_http_codes is not None
            else set(base_retry_codes)
        )
        # Any code we sleep on should also be retried even if it was not
        # included in retry_http_codes.
        self.retry_http_codes = base_retry_codes | sleep_codes
        self.sleep_http_codes = sleep_codes
        self.backoff_base = backoff_base
        self.logger = get_logger(component="RetryMiddleware")

    async def process_response(
        self,
        response: Response,
        spider: Spider,
    ) -> Response | Request:
        request = response.request
        if response.status not in self.retry_http_codes:
            return response

        retry_raw = request.meta.get("retry_times", 0)
        retry_times = retry_raw if isinstance(retry_raw, int) else 0
        if retry_times >= self.max_times:
            return response  # give up

        retry_times += 1
        request = request.replace(dont_filter=True, meta={**request.meta})
        request.meta["retry_times"] = retry_times

        delay = self.backoff_base * (2 ** (retry_times - 1))
        self.logger.warning(
            "Retrying request",
            url=request.url,
            delay=round(delay, 2),
            attempt=retry_times,
            status=response.status,
        )
        if response.status in self.sleep_http_codes and delay > 0:
            # non-blocking sleep to avoid stalling other concurrent fetches
            await asyncio.sleep(delay)

        return request
