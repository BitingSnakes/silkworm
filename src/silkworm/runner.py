from __future__ import annotations
import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from .middlewares import RequestMiddleware, ResponseMiddleware
    from .pipelines import ItemPipeline
    from .spiders import Spider

from .engine import Engine


def _install_uvloop() -> Callable[[], asyncio.AbstractEventLoop]:
    """Return a uvloop event loop factory if available."""
    try:
        import uvloop  # type: ignore[import]

        policy = uvloop.EventLoopPolicy()
        return policy.new_event_loop
    except ImportError as err:
        msg = (
            "uvloop is not installed. Install it with: pip install silkworm-rs[uvloop]"
        )
        raise ImportError(msg) from err


def _install_winloop() -> Callable[[], asyncio.AbstractEventLoop]:
    """Return a winloop event loop factory if available."""
    try:
        import winloop  # type: ignore[import]

        policy = winloop.EventLoopPolicy()
        return policy.new_event_loop
    except ImportError as err:
        msg = "winloop is not installed. Install it with: pip install silkworm-rs[winloop]"
        raise ImportError(msg) from err


def run_spider_trio(
    spider_cls: type[Spider],
    *,
    concurrency: int = 16,
    request_middlewares: Iterable[RequestMiddleware] | None = None,
    response_middlewares: Iterable[ResponseMiddleware] | None = None,
    item_pipelines: Iterable[ItemPipeline] | None = None,
    request_timeout: float | timedelta | None = None,
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
    except ImportError as err:
        msg = "trio is not installed. Install it with: pip install silkworm-rs[trio]"
        raise ImportError(msg) from err

    # Trio uses its own async primitives, but the engine uses asyncio primitives
    # We use trio-asyncio to run asyncio code within trio
    try:
        import trio_asyncio  # type: ignore[import]
    except ImportError as err:
        msg = "trio-asyncio is required for trio support. Install it with: pip install silkworm-rs[trio]"
        raise ImportError(msg) from err

    async def run_with_trio_asyncio():
        async with trio_asyncio.open_loop():
            # Run the asyncio-based crawl coroutine within Trio's event loop.
            # This ensures asyncio TaskGroups have a parent task.
            await trio_asyncio.aio_as_trio(crawl)(
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
    request_timeout: float | timedelta | None = None,
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
    request_timeout: float | timedelta | None = None,
    log_stats_interval: float | None = None,
    max_pending_requests: int | None = None,
    html_max_size_bytes: int = 5_000_000,
    keep_alive: bool = False,
    loop_factory: Callable[[], asyncio.AbstractEventLoop] | None = None,
    **spider_kwargs,
) -> None:
    coroutine = crawl(
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
    if loop_factory is None:
        asyncio.run(coroutine)
        return

    with asyncio.Runner(loop_factory=loop_factory) as runner:
        runner.run(coroutine)


def run_spider_uvloop(
    spider_cls: type[Spider],
    *,
    concurrency: int = 16,
    request_middlewares: Iterable[RequestMiddleware] | None = None,
    response_middlewares: Iterable[ResponseMiddleware] | None = None,
    item_pipelines: Iterable[ItemPipeline] | None = None,
    request_timeout: float | timedelta | None = None,
    log_stats_interval: float | None = None,
    max_pending_requests: int | None = None,
    html_max_size_bytes: int = 5_000_000,
    keep_alive: bool = False,
    **spider_kwargs,
) -> None:
    loop_factory = _install_uvloop()
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
        loop_factory=loop_factory,
        **spider_kwargs,
    )


def run_spider_winloop(
    spider_cls: type[Spider],
    *,
    concurrency: int = 16,
    request_middlewares: Iterable[RequestMiddleware] | None = None,
    response_middlewares: Iterable[ResponseMiddleware] | None = None,
    item_pipelines: Iterable[ItemPipeline] | None = None,
    request_timeout: float | timedelta | None = None,
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
    loop_factory = _install_winloop()
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
        loop_factory=loop_factory,
        **spider_kwargs,
    )
