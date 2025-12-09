"""Tests for trio runner functionality."""

from unittest.mock import patch, MagicMock
import pytest

from silkworm.runner import run_spider_trio
from silkworm.spiders import Spider


class SimpleSpider(Spider):
    """A minimal spider for testing."""

    name = "simple"
    start_urls: tuple[str, ...] = ()

    async def parse(self, response):
        yield {}


def test_run_spider_trio_raises_when_trio_not_installed():
    """Test that ImportError is raised when trio is not installed."""
    import builtins

    original_import = builtins.__import__

    def mock_import(name, *args, **kwargs):
        if name == "trio":
            raise ImportError("No module named 'trio'")
        return original_import(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=mock_import):
        with pytest.raises(ImportError, match="trio is not installed"):
            run_spider_trio(SimpleSpider, concurrency=1)


def test_run_spider_trio_raises_when_trio_asyncio_not_installed():
    """Test that ImportError is raised when trio-asyncio is not installed."""
    mock_trio = MagicMock()

    import builtins

    original_import = builtins.__import__

    def mock_import(name, *args, **kwargs):
        if name == "trio":
            return mock_trio
        if name == "trio_asyncio":
            raise ImportError("No module named 'trio_asyncio'")
        return original_import(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=mock_import):
        with pytest.raises(ImportError, match="trio-asyncio is required"):
            run_spider_trio(SimpleSpider, concurrency=1)


def test_run_spider_trio_with_trio_installed():
    """Test that run_spider_trio works when trio and trio-asyncio are installed."""
    mock_trio = MagicMock()
    mock_trio_asyncio = MagicMock()
    mock_trio.run = MagicMock()

    import builtins

    original_import = builtins.__import__

    def mock_import(name, *args, **kwargs):
        if name == "trio":
            return mock_trio
        if name == "trio_asyncio":
            return mock_trio_asyncio
        return original_import(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=mock_import):
        with patch("silkworm.runner.crawl") as mock_crawl:
            mock_crawl.return_value = None
            run_spider_trio(SimpleSpider, concurrency=1)

            # Verify trio.run was called
            assert mock_trio.run.called
