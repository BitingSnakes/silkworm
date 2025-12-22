"""Tests for winloop runner functionality."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from silkworm.runner import run_spider_winloop, _install_winloop
from silkworm.spiders import Spider


class SimpleSpider(Spider):
    """A minimal spider for testing."""

    name = "simple"
    start_urls: tuple[str, ...] = ()

    async def parse(self, response):
        yield {}


def test_install_winloop_when_available():
    """Test that winloop is installed when available."""
    mock_winloop = MagicMock()
    mock_policy = MagicMock()
    mock_winloop.EventLoopPolicy.return_value = mock_policy

    with patch.dict("sys.modules", {"winloop": mock_winloop}):
        loop_factory = _install_winloop()
        mock_winloop.EventLoopPolicy.assert_called_once_with()
        assert loop_factory is mock_policy.new_event_loop


def test_install_winloop_raises_when_not_installed():
    """Test that ImportError is raised when winloop is not installed."""
    import builtins

    original_import = builtins.__import__

    def mock_import(name, *args, **kwargs):
        if name == "winloop":
            raise ImportError("No module named 'winloop'")
        return original_import(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=mock_import):
        with pytest.raises(ImportError, match="winloop is not installed"):
            _install_winloop()


def test_run_spider_with_winloop_enabled():
    """Test that run_spider_winloop installs winloop policy before running."""
    mock_winloop = MagicMock()
    mock_policy = MagicMock()
    mock_winloop.EventLoopPolicy.return_value = mock_policy

    with patch.dict("sys.modules", {"winloop": mock_winloop}):
        runner_instance = MagicMock()

        def _run_and_close(coro):
            coro.close()

        runner_instance.run.side_effect = _run_and_close

        runner_cm = MagicMock()
        runner_cm.__enter__.return_value = runner_instance
        runner_cm.__exit__.return_value = False

        with patch("asyncio.Runner", return_value=runner_cm) as mock_runner:
            run_spider_winloop(SimpleSpider, concurrency=1)

            mock_runner.assert_called_once_with(loop_factory=mock_policy.new_event_loop)
            runner_instance.run.assert_called_once()


def test_run_spider_with_winloop_not_installed():
    """Test that run_spider_winloop raises error when winloop is missing."""
    import builtins

    original_import = builtins.__import__

    def mock_import(name, *args, **kwargs):
        if name == "winloop":
            raise ImportError("No module named 'winloop'")
        return original_import(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=mock_import):
        with pytest.raises(ImportError, match="winloop is not installed"):
            run_spider_winloop(SimpleSpider, concurrency=1)
