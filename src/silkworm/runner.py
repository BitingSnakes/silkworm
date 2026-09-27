"""Entry points that run a spider to completion.

Every runner takes a spider and the same keyword options (see
:class:`~silkworm.engine.EngineOptions`); they differ only in the event loop::

    run_spider(QuotesSpider, concurrency=32, request_timeout=10)
    run_spider_uvloop(SitemapSpider(sitemap_url=url, max_pages=5), keep_alive=True)

Pass a spider class to use its no-argument constructor, or an instance when the
spider takes arguments, so they are type-checked against its ``__init__``.
"""

from __future__ import annotations

import asyncio
import signal
import sys
import threading
from collections.abc import Callable
from contextlib import contextmanager
from typing import TYPE_CHECKING, Unpack

from .engine import Engine, EngineOptions
from .settings import resolve_options

if TYPE_CHECKING:
    from collections.abc import Iterator

    from ._stats import CrawlResult
    from .spiders import Spider

type LoopFactory = Callable[[], asyncio.AbstractEventLoop]


def _install_uvloop() -> LoopFactory:
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


def _install_rsloop() -> LoopFactory:
    """Return an rsloop event loop factory if available."""
    try:
        import rsloop  # type: ignore[import]

        return rsloop.new_event_loop
    except ImportError as err:
        msg = (
            "rsloop is not installed. Install it with: pip install silkworm-rs[rsloop]"
        )
        raise ImportError(msg) from err


def _install_winloop() -> LoopFactory:
    """Return a winloop event loop factory if available."""
    try:
        import winloop  # type: ignore[import]

        policy = winloop.EventLoopPolicy()
        return policy.new_event_loop
    except ImportError as err:
        msg = (
            "winloop is not installed. "
            "Install it with: pip install silkworm-rs[winloop]"
        )
        raise ImportError(msg) from err


def _as_spider(spider: Spider | type[Spider]) -> Spider:
    return spider() if isinstance(spider, type) else spider


class _ShutdownSignals:
    """Translate SIGINT/SIGTERM into a graceful engine stop, then a forced one."""

    def __init__(self, engine: Engine, task: asyncio.Task[object] | None) -> None:
        self.engine = engine
        self.task = task
        self.received = 0
        self.forced = False

    def handle(self, signal_name: str) -> None:
        self.received += 1
        if self.received == 1:
            self.engine.logger.warning(
                "Received shutdown signal; finishing in-flight requests "
                "(send it again to stop immediately)",
                signal=signal_name,
            )
            self.engine.stop("shutdown")
            return
        self.engine.logger.warning(
            "Received second shutdown signal; cancelling the crawl",
            signal=signal_name,
        )
        self.forced = True
        if self.task is not None:
            self.task.cancel()


@contextmanager
def _graceful_shutdown(engine: Engine) -> Iterator[_ShutdownSignals]:
    """Install SIGINT/SIGTERM handlers for the duration of a crawl."""
    loop = asyncio.get_running_loop()
    handler = _ShutdownSignals(engine, asyncio.current_task())
    loop_signals: list[signal.Signals] = []
    previous: dict[signal.Signals, object] = {}
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handler.handle, sig.name)
            loop_signals.append(sig)
        except (NotImplementedError, RuntimeError, ValueError):
            # e.g. Windows event loops: fall back to plain signal handlers,
            # which only work in the main thread.
            if threading.current_thread() is not threading.main_thread():
                continue

            def forward(signum: int, _frame: object) -> None:
                loop.call_soon_threadsafe(handler.handle, signal.Signals(signum).name)

            previous[sig] = signal.signal(sig, forward)
    try:
        yield handler
    finally:
        for sig in loop_signals:
            loop.remove_signal_handler(sig)
        for sig, old_handler in previous.items():
            signal.signal(sig, old_handler)  # type: ignore[arg-type]


async def crawl(
    spider: Spider | type[Spider],
    *,
    handle_signals: bool = False,
    **options: Unpack[EngineOptions],
) -> CrawlResult:
    """Run ``spider`` to completion on the current event loop.

    Options are resolved through :func:`silkworm.settings.resolve_options`, so
    ``SILKWORM_*`` environment variables and the spider's ``custom_settings``
    apply beneath the options passed here.

    Args:
        spider: Spider instance, or a no-argument spider class.
        handle_signals: Stop gracefully on the first SIGINT/SIGTERM and cancel
            on the second. Off by default because the calling application owns
            the event loop and may handle signals itself.
        **options: :class:`~silkworm.engine.EngineOptions` forwarded to the
            engine.

    Returns:
        The crawl's :class:`~silkworm.CrawlResult`.

    Raises:
        CrawlFailedError: If the crawl violated its failure policy.
        KeyboardInterrupt: If a second shutdown signal forced cancellation.

    Use this coroutine when the application already owns an event loop; use a
    ``run_spider*`` function from synchronous code.
    """
    instance = _as_spider(spider)
    engine = Engine(instance, **resolve_options(instance, options))
    if not handle_signals:
        return await engine.run()
    with _graceful_shutdown(engine) as signals:
        try:
            return await engine.run()
        except asyncio.CancelledError:
            if signals.forced:
                raise KeyboardInterrupt from None
            raise


def run_spider(
    spider: Spider | type[Spider],
    *,
    loop_factory: LoopFactory | None = None,
    handle_signals: bool = True,
    **options: Unpack[EngineOptions],
) -> CrawlResult:
    """
    Run ``spider`` with ``asyncio``, blocking until the crawl finishes.

    The first SIGINT (Ctrl+C) or SIGTERM stops the crawl gracefully: pending
    requests are discarded (or saved with ``job_dir``), in-flight requests
    finish, and pipelines close normally. A second signal cancels immediately.

    Args:
        spider: Spider instance, or a spider class to instantiate without arguments.
        loop_factory: Optional event loop factory, e.g. from uvloop.
        handle_signals: Install the graceful shutdown handlers described above.
        **options: Engine options; see :class:`~silkworm.engine.EngineOptions`.

    Returns:
        The crawl's :class:`~silkworm.CrawlResult`.

    Raises:
        CrawlFailedError: If the crawl violated its failure policy.
    """
    coroutine = crawl(spider, handle_signals=handle_signals, **options)
    if loop_factory is None:
        return asyncio.run(coroutine)

    with asyncio.Runner(loop_factory=loop_factory) as runner:
        return runner.run(coroutine)


def run_spider_uvloop(
    spider: Spider | type[Spider],
    **options: Unpack[EngineOptions],
) -> CrawlResult:
    """
    Run ``spider`` on a uvloop event loop (``pip install silkworm-rs[uvloop]``).

    Raises:
        ImportError: If uvloop is not installed.
    """
    return run_spider(spider, loop_factory=_install_uvloop(), **options)


def run_spider_winloop(
    spider: Spider | type[Spider],
    **options: Unpack[EngineOptions],
) -> CrawlResult:
    """
    Run ``spider`` on a winloop event loop, optimized for Windows
    (``pip install silkworm-rs[winloop]``).

    Raises:
        ImportError: If winloop is not installed.
    """
    return run_spider(spider, loop_factory=_install_winloop(), **options)


def run_spider_rsloop(
    spider: Spider | type[Spider],
    **options: Unpack[EngineOptions],
) -> CrawlResult:
    """
    Run ``spider`` on an rsloop event loop (``pip install silkworm-rs[rsloop]``).

    Raises:
        ImportError: If rsloop is not installed.
    """
    return run_spider(spider, loop_factory=_install_rsloop(), **options)


def run_spider_trio(
    spider: Spider | type[Spider],
    **options: Unpack[EngineOptions],
) -> CrawlResult:
    """
    Run ``spider`` with trio as the async backend (``pip install silkworm-rs[trio]``).

    The engine uses asyncio primitives, so it runs inside trio via trio-asyncio.
    This runner is currently available on Python 3.13 only because
    trio-asyncio 0.16 is incompatible with Python 3.14 and newer.

    Raises:
        ImportError: If trio or trio-asyncio is not installed.
    """
    try:
        import trio  # type: ignore[import]
    except ImportError as err:
        msg = "trio is not installed. Install it with: pip install silkworm-rs[trio]"
        raise ImportError(msg) from err

    try:
        import trio_asyncio  # type: ignore[import]
    except ImportError as err:
        if sys.version_info >= (3, 14):
            # The trio extra skips trio-asyncio here; see pyproject.toml.
            version = f"{sys.version_info.major}.{sys.version_info.minor}"
            msg = (
                f"trio support is not available on Python {version} yet: trio-asyncio "
                "has no compatible release."
            )
        else:
            msg = (
                "trio-asyncio is required for trio support. "
                "Install it with: pip install silkworm-rs[trio]"
            )
        raise ImportError(msg) from err
    except AttributeError as err:
        # trio-asyncio <= 0.16 subclasses asyncio policy classes that Python 3.14
        # removed, so importing it fails with AttributeError.
        version = f"{sys.version_info.major}.{sys.version_info.minor}"
        msg = (
            f"The installed trio-asyncio does not support Python {version}; "
            "trio support needs a trio-asyncio release compatible with it."
        )
        raise ImportError(msg) from err

    async def run_with_trio_asyncio() -> CrawlResult:
        async with trio_asyncio.open_loop():
            # Run the asyncio-based crawl within trio's event loop so asyncio
            # TaskGroups have a parent task. trio handles Ctrl+C itself.
            return await trio_asyncio.aio_as_trio(crawl)(spider, **options)

    return trio.run(run_with_trio_asyncio)


__all__ = [
    "LoopFactory",
    "crawl",
    "run_spider",
    "run_spider_rsloop",
    "run_spider_trio",
    "run_spider_uvloop",
    "run_spider_winloop",
]
