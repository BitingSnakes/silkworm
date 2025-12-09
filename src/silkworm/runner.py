from __future__ import annotations
import asyncio
from typing import Iterable, Type

from .spiders import Spider
from .engine import Engine
from .middlewares import RequestMiddleware, ResponseMiddleware
from .pipelines import ItemPipeline


async def crawl(
    spider_cls: Type[Spider],
    *,
    request_middlewares: Iterable[RequestMiddleware] | None = None,
    response_middlewares: Iterable[ResponseMiddleware] | None = None,
    item_pipelines: Iterable[ItemPipeline] | None = None,
    request_timeout: float | None = None,
    log_stats_interval: float | None = None,
    max_pending_requests: int | None = None,
    **spider_kwargs,
) -> None:
    spider = spider_cls(**spider_kwargs)
    engine = Engine(
        spider,
        request_middlewares=request_middlewares,
        response_middlewares=response_middlewares,
        item_pipelines=item_pipelines,
        request_timeout=request_timeout,
        log_stats_interval=log_stats_interval,
        max_pending_requests=max_pending_requests,
    )
    await engine.run()


def run_spider(
    spider_cls: Type[Spider],
    *,
    request_middlewares: Iterable[RequestMiddleware] | None = None,
    response_middlewares: Iterable[ResponseMiddleware] | None = None,
    item_pipelines: Iterable[ItemPipeline] | None = None,
    request_timeout: float | None = None,
    log_stats_interval: float | None = None,
    max_pending_requests: int | None = None,
    **spider_kwargs,
) -> None:
    asyncio.run(
        crawl(
            spider_cls,
            request_middlewares=request_middlewares,
            response_middlewares=response_middlewares,
            item_pipelines=item_pipelines,
            request_timeout=request_timeout,
            log_stats_interval=log_stats_interval,
            max_pending_requests=max_pending_requests,
            **spider_kwargs,
        )
    )
