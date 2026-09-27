from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from ..exceptions import HttpConnectionError, HttpTimeoutError
from ..logging import Logger, get_logger
from ..request import Request
from ..response import Response

if TYPE_CHECKING:
    from collections.abc import Iterable

    from ..spiders import Spider

# Transport failures retried by default: timeouts and connection errors are
# usually transient, unlike redirect loops, oversized bodies, or callback bugs.
DEFAULT_RETRY_EXCEPTIONS: tuple[type[BaseException], ...] = (
    HttpTimeoutError,
    HttpConnectionError,
    TimeoutError,
    ConnectionError,
)


class RetryMiddleware:
    """Retry failed requests with optional exponential backoff.

    Retries both responses with selected HTTP statuses (as a response
    middleware) and transient transport failures such as timeouts and
    connection resets (as an exception middleware).

    Args:
        max_times: Maximum retries after the initial request.
        retry_http_codes: Status codes that produce a replacement request.
        backoff_base: Base seconds for ``base * 2 ** (attempt - 1)``.
        sleep_http_codes: Retry statuses that also wait before enqueueing. These
            codes are automatically added to the retry set.
        retry_exceptions: Exception types retried with backoff. Defaults to
            :data:`DEFAULT_RETRY_EXCEPTIONS`; pass ``()`` to retry statuses only.

    Attempts are stored in ``request.meta["retry_times"]``, shared by status
    and exception retries, and retry requests bypass deduplication.
    """

    def __init__(
        self,
        max_times: int = 3,
        retry_http_codes: Iterable[int] | None = None,
        backoff_base: float = 0.5,
        sleep_http_codes: Iterable[int] | None = None,
        retry_exceptions: Iterable[type[BaseException]] | None = None,
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
        self.retry_http_codes: set[int] = base_retry_codes | sleep_codes
        self.sleep_http_codes = sleep_codes
        self.backoff_base = backoff_base
        self.retry_exceptions: tuple[type[BaseException], ...] = (
            tuple(retry_exceptions)
            if retry_exceptions is not None
            else DEFAULT_RETRY_EXCEPTIONS
        )
        self.logger: Logger = get_logger(component="RetryMiddleware")

    def _next_attempt(self, request: Request) -> tuple[Request, int, float] | None:
        """Return ``(retry_request, attempt, delay)`` or ``None`` at the limit."""
        retry_raw = request.meta.get("retry_times", 0)
        retry_times = retry_raw if isinstance(retry_raw, int) else 0
        if retry_times >= self.max_times:
            return None
        retry_times += 1
        retry = request.replace(dont_filter=True, meta={**request.meta})
        retry.meta["retry_times"] = retry_times
        return retry, retry_times, self.backoff_base * (2 ** (retry_times - 1))

    async def process_exception(
        self,
        request: Request,
        exception: Exception,
        spider: Spider,
    ) -> Request | None:
        """Return a retry request for transient transport failures.

        The retry waits the exponential backoff delay first. Returns ``None``
        for other exceptions or once ``max_times`` retries were made.
        """
        if not isinstance(exception, self.retry_exceptions):
            return None
        attempt = self._next_attempt(request)
        if attempt is None:
            self.logger.warning(
                "Giving up retrying request",
                url=request.url,
                attempts=self.max_times,
                error=str(exception),
                error_type=exception.__class__.__name__,
            )
            return None
        retry, retry_times, delay = attempt
        self.logger.warning(
            "Retrying request",
            url=request.url,
            delay=round(delay, 2),
            attempt=retry_times,
            error_type=exception.__class__.__name__,
        )
        if delay > 0:
            await asyncio.sleep(delay)
        return retry

    async def process_response(
        self,
        response: Response,
        spider: Spider,
    ) -> Response | Request:
        """Return a retry request for eligible statuses until the limit is met."""
        request = response.request
        if response.status not in self.retry_http_codes:
            return response

        attempt = self._next_attempt(request)
        if attempt is None:
            return response  # give up

        request, retry_times, delay = attempt
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
