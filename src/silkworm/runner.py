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
import sys
from collections.abc import Callable
from typing import TYPE_CHECKING, Unpack

from .engine import Engine, EngineOptions

if TYPE_CHECKING:
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


async def crawl(
    spider: Spider | type[Spider],
    **options: Unpack[EngineOptions],
) -> None:
    """Run ``spider`` to completion on the current event loop.

    Args:
        spider: Spider instance, or a no-argument spider class.
        **options: :class:`~silkworm.engine.EngineOptions` forwarded to the
            engine.

    Use this coroutine when the application already owns an event loop; use a
    ``run_spider*`` function from synchronous code.
    """
    await Engine(_as_spider(spider), **options).run()


def run_spider(
    spider: Spider | type[Spider],
    *,
    loop_factory: LoopFactory | None = None,
    **options: Unpack[EngineOptions],
) -> None:
    """
    Run ``spider`` with ``asyncio``, blocking until the crawl finishes.

    Args:
        spider: Spider instance, or a spider class to instantiate without arguments.
        loop_factory: Optional event loop factory, e.g. from uvloop.
        **options: Engine options; see :class:`~silkworm.engine.EngineOptions`.
    """
    coroutine = crawl(spider, **options)
    if loop_factory is None:
        asyncio.run(coroutine)
        return

    with asyncio.Runner(loop_factory=loop_factory) as runner:
        runner.run(coroutine)


def run_spider_uvloop(
    spider: Spider | type[Spider],
    **options: Unpack[EngineOptions],
) -> None:
    """
    Run ``spider`` on a uvloop event loop (``pip install silkworm-rs[uvloop]``).

    Raises:
        ImportError: If uvloop is not installed.
    """
    run_spider(spider, loop_factory=_install_uvloop(), **options)


def run_spider_winloop(
    spider: Spider | type[Spider],
    **options: Unpack[EngineOptions],
) -> None:
    """
    Run ``spider`` on a winloop event loop, optimized for Windows
    (``pip install silkworm-rs[winloop]``).

    Raises:
        ImportError: If winloop is not installed.
    """
    run_spider(spider, loop_factory=_install_winloop(), **options)


def run_spider_rsloop(
    spider: Spider | type[Spider],
    **options: Unpack[EngineOptions],
) -> None:
    """
    Run ``spider`` on an rsloop event loop (``pip install silkworm-rs[rsloop]``).

    Raises:
        ImportError: If rsloop is not installed.
    """
    run_spider(spider, loop_factory=_install_rsloop(), **options)


def run_spider_trio(
    spider: Spider | type[Spider],
    **options: Unpack[EngineOptions],
) -> None:
    """
    Run ``spider`` with trio as the async backend (``pip install silkworm-rs[trio]``).

    The engine uses asyncio primitives, so it runs inside trio via trio-asyncio.

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
        if sys.version_info >= (3, 15):
            # The trio extra skips trio-asyncio here; see pyproject.toml.
            msg = (
                "trio support is not available on Python 3.15 yet: trio-asyncio "
                "has no compatible release."
            )
        else:
            msg = (
                "trio-asyncio is required for trio support. "
                "Install it with: pip install silkworm-rs[trio]"
            )
        raise ImportError(msg) from err
    except AttributeError as err:
        # trio-asyncio <= 0.16 subclasses asyncio policy classes that Python 3.15
        # removed, so importing it fails with AttributeError.
        version = f"{sys.version_info.major}.{sys.version_info.minor}"
        msg = (
            f"The installed trio-asyncio does not support Python {version}; "
            "trio support needs a trio-asyncio release compatible with it."
        )
        raise ImportError(msg) from err

    async def run_with_trio_asyncio() -> None:
        async with trio_asyncio.open_loop():
            # Run the asyncio-based crawl within trio's event loop so asyncio
            # TaskGroups have a parent task.
            await trio_asyncio.aio_as_trio(crawl)(spider, **options)

    trio.run(run_with_trio_asyncio)


__all__ = [
    "LoopFactory",
    "crawl",
    "run_spider",
    "run_spider_rsloop",
    "run_spider_trio",
    "run_spider_uvloop",
    "run_spider_winloop",
]
