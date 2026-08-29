from __future__ import annotations

from typing import cast, override

from silkworm import Request, Response, Spider, run_spider
from silkworm._types import JSONValue
from silkworm.http import MOCK_RESPONSE_META_KEY
from silkworm.middlewares import RequestMiddleware
from silkworm.pipelines import JsonLinesPipeline
from silkworm.request import CallbackOutput


class FailThenRecoverMiddleware:
    """
    Force two local failures so the example can demonstrate retry and errback
    behavior without relying on an unreliable remote endpoint.
    """

    async def process_request(self, request: Request, spider: Spider) -> Request:
        if request.meta.get("force_failure") is not True:
            return request

        if request.meta.get("failed_once") is True:
            return request

        raise RuntimeError(f"Forced failure for {request.url}")

    async def process_exception(
        self,
        request: Request,
        exception: Exception,
        spider: Spider,
    ) -> Request | None:
        if request.meta.get("retry_once") is not True:
            spider.log.warning(
                "Leaving exception for request errback",
                url=request.url,
                error_type=exception.__class__.__name__,
            )
            return None

        spider.log.info(
            "Retrying failed request from exception middleware",
            url=request.url,
            error_type=exception.__class__.__name__,
        )
        return request.replace(
            dont_filter=True,
            meta={
                **request.meta,
                "failed_once": True,
                MOCK_RESPONSE_META_KEY: cast(
                    "JSONValue",
                    {
                        "url": request.url,
                        "status": 200,
                        "headers": {"content-type": "text/plain; charset=utf-8"},
                        "body": "Recovered by process_exception",
                    },
                ),
            },
        )


class ExceptionHandlingSpider(Spider):
    name = "exception_handling"

    async def start_requests(self):
        yield Request(
            url="https://example.test/retry",
            callback=self.parse,
            errback=self.handle_error,
            meta={"force_failure": True, "retry_once": True},
        )
        yield Request(
            url="https://example.test/errback",
            callback=self.parse,
            errback=self.handle_error,
            meta={"force_failure": True},
        )

    @override
    async def parse(self, response: Response) -> CallbackOutput:
        yield {
            "url": response.url,
            "status": response.status,
            "source": "callback",
            "body": response.text,
        }

    async def handle_error(
        self,
        request: Request,
        exception: Exception,
    ) -> CallbackOutput:
        yield {
            "url": request.url,
            "source": "errback",
            "error_type": exception.__class__.__name__,
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
