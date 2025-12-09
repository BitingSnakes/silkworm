from __future__ import annotations
import asyncio
from collections.abc import Iterable

from .spiders import Spider
from .engine import Engine
from .middlewares import RequestMiddleware, ResponseMiddleware
from .pipelines import ItemPipeline


def _install_uvloop() -> None:
    """Install uvloop event loop policy if available."""
    try:
        import uvloop  # type: ignore[import]
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ImportError:
        raise ImportError(
            "uvloop is not installed. Install it with: pip install silkworm-rs[uvloop]"
        )


async def crawl(
    spider_cls: type[Spider],
    *,
    request_middlewares: Iterable[RequestMiddleware] | None = None,
    response_middlewares: Iterable[ResponseMiddleware] | None = None,
    item_pipelines: Iterable[ItemPipeline] | None = None,
    request_timeout: float | None = None,
    log_stats_interval: float | None = None,
    max_pending_requests: int | None = None,
    html_max_size_bytes: int = 5_000_000,
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
        html_max_size_bytes=html_max_size_bytes,
    )
    await engine.run()


def run_spider(
    spider_cls: type[Spider],
    *,
    request_middlewares: Iterable[RequestMiddleware] | None = None,
    response_middlewares: Iterable[ResponseMiddleware] | None = None,
    item_pipelines: Iterable[ItemPipeline] | None = None,
    request_timeout: float | None = None,
    log_stats_interval: float | None = None,
    max_pending_requests: int | None = None,
    html_max_size_bytes: int = 5_000_000,
    use_uvloop: bool = False,
    **spider_kwargs,
) -> None:
    if use_uvloop:
        _install_uvloop()
    
    asyncio.run(
        crawl(
            spider_cls,
            request_middlewares=request_middlewares,
            response_middlewares=response_middlewares,
            item_pipelines=item_pipelines,
            request_timeout=request_timeout,
            log_stats_interval=log_stats_interval,
            max_pending_requests=max_pending_requests,
            html_max_size_bytes=html_max_size_bytes,
            **spider_kwargs,
        )
    )
