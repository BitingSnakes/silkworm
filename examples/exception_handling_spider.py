"""
Handle errors: retry a failed request, or catch the error in an "errback".

When a request fails (network error, timeout, a middleware raises an error...),
Silkworm gives you two places to react:

1. A middleware's `process_exception()` method. It can return a NEW request to
   try again, or return None to let the error continue.
2. The request's `errback`. Like `callback`, but called with the error instead
   of a response. You can log it or yield an item describing the failure.

This example does not use the internet. A small middleware fakes the failures
so you get the same result every time:

    /retry    fails once -> process_exception retries it -> parse() gets a response
    /errback  fails      -> process_exception gives up   -> handle_error() is called

How to run:
    python examples/exception_handling_spider.py

Output:
    data/exception_handling.jl  (one item from parse, one from handle_error)
"""

from __future__ import annotations

from silkworm import Request, Response, Spider, run_spider
from silkworm.http import MOCK_RESPONSE_META_KEY
from silkworm.middlewares import RequestMiddleware
from silkworm.pipelines import JsonLinesPipeline


class FailThenRecoverMiddleware:
    """
    A demo middleware that makes some requests fail on purpose.

    Requests with meta["force_failure"] = True raise an error the first time.
    """

    async def process_request(self, request: Request, spider: Spider) -> Request:
        """Called before every request is sent."""
        should_fail = request.meta.get("force_failure") is True
        already_failed = request.meta.get("failed_once") is True

        if should_fail and not already_failed:
            raise RuntimeError(f"Forced failure for {request.url}")

        return request

    async def process_exception(
        self,
        request: Request,
        exception: Exception,
        spider: Spider,
    ) -> Request | None:
        """Called when a request raised an error."""
        # Only retry requests that asked for it.
        if request.meta.get("retry_once") is not True:
            spider.log.warning("Giving up, the errback will handle it", url=request.url)
            return None  # None = "I didn't fix it", so the errback is called.

        spider.log.info("Retrying failed request", url=request.url)

        # Build a new request to try again. `replace()` copies the request and
        # changes only the fields we pass in.
        new_meta = dict(request.meta)
        new_meta["failed_once"] = True
        # The example URLs don't exist, so we ask Silkworm to return this fake
        # response instead of really downloading the page.
        new_meta[MOCK_RESPONSE_META_KEY] = {
            "url": request.url,
            "status": 200,
            "headers": {"content-type": "text/plain; charset=utf-8"},
            "body": "Recovered by process_exception",
        }
        # dont_filter=True: allow the same URL to be requested a second time.
        return request.replace(dont_filter=True, meta=new_meta)


class ExceptionHandlingSpider(Spider):
    name = "exception_handling"

    async def start_requests(self):
        # This one fails once, then is retried and succeeds.
        yield Request(
            url="https://example.test/retry",
            callback=self.parse,
            errback=self.handle_error,
            meta={"force_failure": True, "retry_once": True},
        )
        # This one fails and is NOT retried, so handle_error() is called.
        yield Request(
            url="https://example.test/errback",
            callback=self.parse,
            errback=self.handle_error,
            meta={"force_failure": True},
        )

    async def parse(self, response: Response):
        """Called when a request succeeds."""
        yield {
            "url": response.url,
            "status": response.status,
            "source": "callback",
            "body": response.text,
        }

    async def handle_error(self, request: Request, exception: Exception):
        """Called when a request fails for good."""
        yield {
            "url": request.url,
            "source": "errback",
            "error_type": type(exception).__name__,
            "error": str(exception),
        }


if __name__ == "__main__":
    request_middlewares: list[RequestMiddleware] = [FailThenRecoverMiddleware()]
    run_spider(
        ExceptionHandlingSpider,
        request_middlewares=request_middlewares,
        item_pipelines=[
            JsonLinesPipeline("data/exception_handling.jl", use_opendal=False),
        ],
        concurrency=1,
    )
