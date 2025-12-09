"""Tests for runner module and uvloop integration."""

import sys
from contextlib import contextmanager
from unittest.mock import patch, MagicMock
import pytest

from silkworm.runner import run_spider, run_spider_uvloop, _install_uvloop
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
        with patch("asyncio.set_event_loop_policy") as mock_set_policy:
            _install_uvloop()
            mock_set_policy.assert_called_once_with(mock_policy)


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

        with patch("builtins.__import__", side_effect=mock_import):
            with pytest.raises(ImportError, match="uvloop is not installed"):
                _install_uvloop()


def test_run_spider_without_uvloop():
    """Test that run_spider works without uvloop (default behavior)."""

    def _run_and_close(coro):
        # Close coroutine to avoid unawaited coroutine warnings when mocking asyncio.run
        coro.close()

    with patch("asyncio.run", side_effect=_run_and_close) as mock_run:
        with patch("silkworm.runner._install_uvloop") as mock_install:
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
        with patch("asyncio.set_event_loop_policy") as mock_set_policy:

            def _run_and_close(coro):
                coro.close()

            with patch("asyncio.run", side_effect=_run_and_close) as mock_run:
                run_spider_uvloop(SimpleSpider, concurrency=1)

                # Verify uvloop policy was set
                mock_set_policy.assert_called_once_with(mock_policy)
                # Verify asyncio.run was still called
                mock_run.assert_called_once()


def test_run_spider_with_uvloop_not_installed():
    """Test that run_spider_uvloop raises error when uvloop is missing."""
    with without_uvloop_module():
        import builtins

        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "uvloop":
                raise ImportError("No module named 'uvloop'")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            with pytest.raises(ImportError, match="uvloop is not installed"):
                run_spider_uvloop(SimpleSpider, concurrency=1)
