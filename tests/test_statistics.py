import pytest

from silkworm.engine import Engine
from silkworm.request import Request
from silkworm.response import Response
from silkworm.spiders import Spider


class MockSpider(Spider):
    name = "mock"
    start_urls = []

    async def parse(self, response):
        yield {"data": "test"}


@pytest.mark.anyio("asyncio")
async def test_engine_tracks_statistics():
    """Test that the engine tracks basic statistics."""
    spider = MockSpider()
    engine = Engine(spider)

    # Check initial statistics
    assert engine._stats["requests_sent"] == 0
    assert engine._stats["responses_received"] == 0
    assert engine._stats["items_scraped"] == 0
    assert engine._stats["errors"] == 0


@pytest.mark.anyio("asyncio")
async def test_engine_increments_item_count():
    """Test that items are counted correctly."""
    spider = MockSpider()
    engine = Engine(spider)

    # Manually increment items to simulate scraping
    await engine._process_item({"test": "data"})
    assert engine._stats["items_scraped"] == 1

    await engine._process_item({"test": "data2"})
    assert engine._stats["items_scraped"] == 2


@pytest.mark.anyio("asyncio")
async def test_engine_initializes_with_log_stats_interval():
    """Test that the engine accepts log_stats_interval parameter."""
    spider = MockSpider()
    engine = Engine(spider, log_stats_interval=5.0)

    assert engine.log_stats_interval == 5.0


@pytest.mark.anyio("asyncio")
async def test_engine_accepts_none_log_stats_interval():
    """Test that the engine accepts None for log_stats_interval."""
    spider = MockSpider()
    engine = Engine(spider, log_stats_interval=None)

    assert engine.log_stats_interval is None
