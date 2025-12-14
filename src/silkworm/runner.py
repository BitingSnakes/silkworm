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


def _install_winloop() -> None:
    """Install winloop event loop policy if available."""
    try:
        import winloop  # type: ignore[import]

        asyncio.set_event_loop_policy(winloop.EventLoopPolicy())
    except ImportError:
        raise ImportError(
            "winloop is not installed. Install it with: pip install silkworm-rs[winloop]"
        )


def run_spider_trio(
    spider_cls: type[Spider],
    *,
    concurrency: int = 16,
    request_middlewares: Iterable[RequestMiddleware] | None = None,
    response_middlewares: Iterable[ResponseMiddleware] | None = None,
    item_pipelines: Iterable[ItemPipeline] | None = None,
    request_timeout: float | None = None,
    log_stats_interval: float | None = None,
    max_pending_requests: int | None = None,
    html_max_size_bytes: int = 5_000_000,
    keep_alive: bool = False,
    **spider_kwargs,
) -> None:
    """
    Run a spider using trio as the async backend.

    This is similar to run_spider but uses trio.run() instead of asyncio.run().
    Trio must be installed separately: pip install silkworm-rs[trio]

    Args:
        spider_cls: Spider class to instantiate and run
        concurrency: Number of concurrent HTTP requests (default: 16)
        request_middlewares: Optional request middlewares
        response_middlewares: Optional response middlewares
        item_pipelines: Optional item pipelines
        request_timeout: Per-request timeout in seconds
        log_stats_interval: Interval for logging statistics
        max_pending_requests: Maximum pending requests in queue
        html_max_size_bytes: Maximum HTML size to parse
        keep_alive: Enable HTTP keep-alive when supported by the HTTP client
        **spider_kwargs: Additional kwargs passed to spider constructor

    Raises:
        ImportError: If trio or trio-asyncio is not installed
    """
    try:
        import trio  # type: ignore[import]
    except ImportError:
        raise ImportError(
            "trio is not installed. Install it with: pip install silkworm-rs[trio]"
        )

    # Trio uses its own async primitives, but the engine uses asyncio primitives
    # We use trio-asyncio to run asyncio code within trio
    try:
        import trio_asyncio  # type: ignore[import]
    except ImportError:
        raise ImportError(
            "trio-asyncio is required for trio support. Install it with: pip install silkworm-rs[trio]"
        )

    async def run_with_trio_asyncio():
        async with trio_asyncio.open_loop():
            # Run the asyncio-based crawl coroutine within the Trio event loop.
            # trio-asyncio 0.14+ allows running asyncio code directly within open_loop()
            await crawl(
                spider_cls,
                concurrency=concurrency,
                request_middlewares=request_middlewares,
                response_middlewares=response_middlewares,
                item_pipelines=item_pipelines,
                request_timeout=request_timeout,
                log_stats_interval=log_stats_interval,
                max_pending_requests=max_pending_requests,
                html_max_size_bytes=html_max_size_bytes,
                keep_alive=keep_alive,
                **spider_kwargs,
            )

    trio.run(run_with_trio_asyncio)


async def crawl(
    spider_cls: type[Spider],
    *,
    concurrency: int = 16,
    request_middlewares: Iterable[RequestMiddleware] | None = None,
    response_middlewares: Iterable[ResponseMiddleware] | None = None,
    item_pipelines: Iterable[ItemPipeline] | None = None,
    request_timeout: float | None = None,
    log_stats_interval: float | None = None,
    max_pending_requests: int | None = None,
    html_max_size_bytes: int = 5_000_000,
    keep_alive: bool = False,
    **spider_kwargs,
) -> None:
    spider = spider_cls(**spider_kwargs)
    engine = Engine(
        spider,
        concurrency=concurrency,
        request_middlewares=request_middlewares,
        response_middlewares=response_middlewares,
        item_pipelines=item_pipelines,
        request_timeout=request_timeout,
        log_stats_interval=log_stats_interval,
        max_pending_requests=max_pending_requests,
        html_max_size_bytes=html_max_size_bytes,
        keep_alive=keep_alive,
    )
    await engine.run()


def run_spider(
    spider_cls: type[Spider],
    *,
    concurrency: int = 16,
    request_middlewares: Iterable[RequestMiddleware] | None = None,
    response_middlewares: Iterable[ResponseMiddleware] | None = None,
    item_pipelines: Iterable[ItemPipeline] | None = None,
    request_timeout: float | None = None,
    log_stats_interval: float | None = None,
    max_pending_requests: int | None = None,
    html_max_size_bytes: int = 5_000_000,
    keep_alive: bool = False,
    **spider_kwargs,
) -> None:
    asyncio.run(
        crawl(
            spider_cls,
            concurrency=concurrency,
            request_middlewares=request_middlewares,
            response_middlewares=response_middlewares,
            item_pipelines=item_pipelines,
            request_timeout=request_timeout,
            log_stats_interval=log_stats_interval,
            max_pending_requests=max_pending_requests,
            html_max_size_bytes=html_max_size_bytes,
            keep_alive=keep_alive,
            **spider_kwargs,
        )
    )


def run_spider_uvloop(
    spider_cls: type[Spider],
    *,
    concurrency: int = 16,
    request_middlewares: Iterable[RequestMiddleware] | None = None,
    response_middlewares: Iterable[ResponseMiddleware] | None = None,
    item_pipelines: Iterable[ItemPipeline] | None = None,
    request_timeout: float | None = None,
    log_stats_interval: float | None = None,
    max_pending_requests: int | None = None,
    html_max_size_bytes: int = 5_000_000,
    keep_alive: bool = False,
    **spider_kwargs,
) -> None:
    _install_uvloop()
    run_spider(
        spider_cls,
        concurrency=concurrency,
        request_middlewares=request_middlewares,
        response_middlewares=response_middlewares,
        item_pipelines=item_pipelines,
        request_timeout=request_timeout,
        log_stats_interval=log_stats_interval,
        max_pending_requests=max_pending_requests,
        html_max_size_bytes=html_max_size_bytes,
        keep_alive=keep_alive,
        **spider_kwargs,
    )


def run_spider_winloop(
    spider_cls: type[Spider],
    *,
    concurrency: int = 16,
    request_middlewares: Iterable[RequestMiddleware] | None = None,
    response_middlewares: Iterable[ResponseMiddleware] | None = None,
    item_pipelines: Iterable[ItemPipeline] | None = None,
    request_timeout: float | None = None,
    log_stats_interval: float | None = None,
    max_pending_requests: int | None = None,
    html_max_size_bytes: int = 5_000_000,
    keep_alive: bool = False,
    **spider_kwargs,
) -> None:
    """
    Run a spider using winloop as the event loop.

    This is similar to run_spider_uvloop but uses winloop instead,
    which is optimized for Windows. Winloop must be installed separately:
    pip install silkworm-rs[winloop]

    Args:
        spider_cls: Spider class to instantiate and run
        concurrency: Number of concurrent HTTP requests (default: 16)
        request_middlewares: Optional request middlewares
        response_middlewares: Optional response middlewares
        item_pipelines: Optional item pipelines
        request_timeout: Per-request timeout in seconds
        log_stats_interval: Interval for logging statistics
        max_pending_requests: Maximum pending requests in queue
        html_max_size_bytes: Maximum HTML size to parse
        keep_alive: Enable HTTP keep-alive when supported by the HTTP client
        **spider_kwargs: Additional kwargs passed to spider constructor

    Raises:
        ImportError: If winloop is not installed
    """
    _install_winloop()
    run_spider(
        spider_cls,
        concurrency=concurrency,
        request_middlewares=request_middlewares,
        response_middlewares=response_middlewares,
        item_pipelines=item_pipelines,
        request_timeout=request_timeout,
        log_stats_interval=log_stats_interval,
        max_pending_requests=max_pending_requests,
        html_max_size_bytes=html_max_size_bytes,
        keep_alive=keep_alive,
        **spider_kwargs,
    )
