"""Tests for rsloop runner functionality."""

from __future__ import annotations

import sys
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from silkworm.runner import run_spider_rsloop, _install_rsloop
from silkworm.spiders import Spider


class SimpleSpider(Spider):
    """A minimal spider for testing."""

    name = "simple"
    start_urls: tuple[str, ...] = ()

    async def parse(self, response):
        yield {}


@contextmanager
def without_rsloop_module():
    """Context manager to temporarily remove rsloop from sys.modules."""
    rsloop_backup = sys.modules.get("rsloop")
    if "rsloop" in sys.modules:
        del sys.modules["rsloop"]

    try:
        yield
    finally:
        if rsloop_backup is not None:
            sys.modules["rsloop"] = rsloop_backup


def test_install_rsloop_when_available():
    """Test that rsloop is installed when available."""
    mock_rsloop = MagicMock()

    with patch.dict("sys.modules", {"rsloop": mock_rsloop}):
        loop_factory = _install_rsloop()
        assert loop_factory is mock_rsloop.new_event_loop


def test_install_rsloop_raises_when_not_installed():
    """Test that ImportError is raised when rsloop is not installed."""
    with without_rsloop_module():
        import builtins

        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "rsloop":
                raise ImportError("No module named 'rsloop'")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            with pytest.raises(ImportError, match="rsloop is not installed"):
                _install_rsloop()


def test_run_spider_with_rsloop_enabled():
    """Test that run_spider_rsloop uses rsloop's loop factory before running."""
    mock_rsloop = MagicMock()

    with patch.dict("sys.modules", {"rsloop": mock_rsloop}):
        runner_instance = MagicMock()

        def _run_and_close(coro):
            coro.close()

        runner_instance.run.side_effect = _run_and_close

        runner_cm = MagicMock()
        runner_cm.__enter__.return_value = runner_instance
        runner_cm.__exit__.return_value = False

        with patch("asyncio.Runner", return_value=runner_cm) as mock_runner:
            run_spider_rsloop(SimpleSpider, concurrency=1)

            mock_runner.assert_called_once_with(loop_factory=mock_rsloop.new_event_loop)
            runner_instance.run.assert_called_once()


def test_run_spider_with_rsloop_not_installed():
    """Test that run_spider_rsloop raises error when rsloop is missing."""
    with without_rsloop_module():
        import builtins

        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "rsloop":
                raise ImportError("No module named 'rsloop'")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            with pytest.raises(ImportError, match="rsloop is not installed"):
                run_spider_rsloop(SimpleSpider, concurrency=1)
