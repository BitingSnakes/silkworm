"""Tests for runner module and uvloop integration."""

from __future__ import annotations

import sys
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from silkworm.runner import _install_uvloop, run_spider, run_spider_uvloop
from silkworm.spiders import Spider


class SimpleSpider(Spider):
    """A minimal spider for testing."""

    name = "simple"
    start_urls: tuple[str, ...] = ()

    async def parse(self, response):
        yield {}


@contextmanager
def without_uvloop_module():
    """Context manager to temporarily remove uvloop from sys.modules."""
    uvloop_backup = sys.modules.get("uvloop")
    if "uvloop" in sys.modules:
        del sys.modules["uvloop"]

    try:
        yield
    finally:
        # Restore uvloop if it was there
        if uvloop_backup is not None:
            sys.modules["uvloop"] = uvloop_backup


def test_install_uvloop_when_available():
    """Test that uvloop is installed when available."""
    mock_uvloop = MagicMock()
    mock_policy = MagicMock()
    mock_uvloop.EventLoopPolicy.return_value = mock_policy

    with patch.dict("sys.modules", {"uvloop": mock_uvloop}):
        loop_factory = _install_uvloop()
        mock_uvloop.EventLoopPolicy.assert_called_once_with()
        assert loop_factory is mock_policy.new_event_loop


def test_install_uvloop_raises_when_not_installed():
    """Test that ImportError is raised when uvloop is not installed."""
    with without_uvloop_module():
        # Mock the import to raise ImportError
        import builtins

        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "uvloop":
                raise ImportError("No module named 'uvloop'")
            return original_import(name, *args, **kwargs)

        with (
            patch("builtins.__import__", side_effect=mock_import),
            pytest.raises(ImportError, match="uvloop is not installed"),
        ):
            _install_uvloop()


def test_run_spider_without_uvloop():
    """Test that run_spider works without uvloop (default behavior)."""

    def _run_and_close(coro):
        # Close coroutine to avoid unawaited coroutine warnings when mocking asyncio.run
        coro.close()

    with (
        patch("asyncio.run", side_effect=_run_and_close) as mock_run,
        patch("silkworm.runner._install_uvloop") as mock_install,
    ):
        run_spider(SimpleSpider, concurrency=1)

        # Verify _install_uvloop was not called
        mock_install.assert_not_called()
        # Verify asyncio.run was called
        mock_run.assert_called_once()


def test_run_spider_with_uvloop_enabled():
    """Test that run_spider_uvloop installs uvloop policy before running."""
    mock_uvloop = MagicMock()
    mock_policy = MagicMock()
    mock_uvloop.EventLoopPolicy.return_value = mock_policy

    with patch.dict("sys.modules", {"uvloop": mock_uvloop}):
        runner_instance = MagicMock()

        def _run_and_close(coro):
            coro.close()

        runner_instance.run.side_effect = _run_and_close

        runner_cm = MagicMock()
        runner_cm.__enter__.return_value = runner_instance
        runner_cm.__exit__.return_value = False

        with patch("asyncio.Runner", return_value=runner_cm) as mock_runner:
            run_spider_uvloop(SimpleSpider, concurrency=1)

            mock_runner.assert_called_once_with(loop_factory=mock_policy.new_event_loop)
            runner_instance.run.assert_called_once()


def test_run_spider_with_uvloop_not_installed():
    """Test that run_spider_uvloop raises error when uvloop is missing."""
    with without_uvloop_module():
        import builtins

        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "uvloop":
                raise ImportError("No module named 'uvloop'")
            return original_import(name, *args, **kwargs)

        with (
            patch("builtins.__import__", side_effect=mock_import),
            pytest.raises(ImportError, match="uvloop is not installed"),
        ):
            run_spider_uvloop(SimpleSpider, concurrency=1)


class ArgSpider(Spider):
    """A spider whose constructor takes its own argument."""

    name = "args"

    def __init__(self, *, pages: int, **kwargs):
        super().__init__(**kwargs)
        self.pages = pages

    async def parse(self, response):
        yield {}


def _record_engines(monkeypatch: pytest.MonkeyPatch) -> list[tuple[Spider, dict]]:
    created: list[tuple[Spider, dict]] = []

    class FakeEngine:
        def __init__(self, spider: Spider, **options) -> None:
            created.append((spider, options))

        async def run(self) -> None:
            return None

    monkeypatch.setattr("silkworm.runner.Engine", FakeEngine)
    return created


def test_run_spider_uses_spider_instance_and_forwards_options(
    monkeypatch: pytest.MonkeyPatch,
):
    created = _record_engines(monkeypatch)
    spider = ArgSpider(pages=3)

    run_spider(spider, concurrency=4, request_timeout=5, emulation=None)

    assert len(created) == 1
    used_spider, options = created[0]
    assert used_spider is spider
    assert spider.pages == 3
    assert options == {"concurrency": 4, "request_timeout": 5, "emulation": None}


def test_run_spider_instantiates_spider_class(monkeypatch: pytest.MonkeyPatch):
    created = _record_engines(monkeypatch)

    run_spider(SimpleSpider)

    used_spider, options = created[0]
    assert isinstance(used_spider, SimpleSpider)
    assert options == {}


def test_engine_options_match_engine_keyword_parameters():
    import inspect

    from silkworm.engine import Engine, EngineOptions

    parameters = inspect.signature(Engine.__init__).parameters
    keyword_only = {
        name
        for name, parameter in parameters.items()
        if parameter.kind is inspect.Parameter.KEYWORD_ONLY
    }
    assert set(EngineOptions.__annotations__) == keyword_only
