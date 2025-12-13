import pytest

from silkworm.engine import Engine
from silkworm.request import CallbackOutput
from silkworm.response import Response
from silkworm.spiders import Spider


class MockSpider(Spider):
    name = "mock"
    start_urls: tuple[str, ...] = ()

    async def parse(self, response: Response) -> CallbackOutput:
        return {"data": "test"}


async def test_engine_tracks_statistics() -> None:
    """Test that the engine tracks basic statistics."""
    spider = MockSpider()
    engine = Engine(spider)

    # Check initial statistics
    assert engine._stats["requests_sent"] == 0
    assert engine._stats["responses_received"] == 0
    assert engine._stats["items_scraped"] == 0
    assert engine._stats["errors"] == 0


async def test_engine_increments_item_count() -> None:
    """Test that items are counted correctly."""
    spider = MockSpider()
    engine = Engine(spider)

    # Manually increment items to simulate scraping
    await engine._process_item({"test": "data"})
    assert engine._stats["items_scraped"] == 1

    await engine._process_item({"test": "data2"})
    assert engine._stats["items_scraped"] == 2


async def test_engine_initializes_with_log_stats_interval() -> None:
    """Test that the engine accepts log_stats_interval parameter."""
    spider = MockSpider()
    engine = Engine(spider, log_stats_interval=5.0)

    assert engine.log_stats_interval == 5.0


async def test_engine_accepts_none_log_stats_interval() -> None:
    """Test that the engine accepts None for log_stats_interval."""
    spider = MockSpider()
    engine = Engine(spider, log_stats_interval=None)

    assert engine.log_stats_interval is None


async def test_engine_stats_payload_includes_seen_and_memory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Engine stats payload includes seen count and memory usage."""
    spider = MockSpider()
    engine = Engine(spider)
    engine._seen.update({"https://a.example", "https://b.example"})
    monkeypatch.setattr(engine, "_get_memory_usage_mb", lambda: 123.456)

    payload = engine._stats_payload(elapsed=2.0)

    assert payload["seen_requests"] == 2
    assert payload["memory_mb"] == 123.46


class _SpyLogger:
    def __init__(self) -> None:
        self.info_calls: list[tuple[str, dict[str, object]]] = []

    def configure(
        self,
        *,
        level: str,
        show_time: bool,
        show_module: bool,
        show_function: bool,
        show_filename: bool,
        show_lineno: bool,
        console_levels: dict[str, bool] | None = None,
    ) -> None:
        return None

    def bind(self, **context: object) -> "_SpyLogger":
        return self

    def info(self, message: str, **kwargs: object) -> None:
        self.info_calls.append((message, kwargs))

    def debug(self, *_args, **_kwargs):  # pragma: no cover - unused here
        return None

    def warning(self, *_args, **_kwargs):  # pragma: no cover - unused here
        return None

    def error(self, *_args, **_kwargs):  # pragma: no cover - unused here
        return None

    def complete(self) -> None:  # pragma: no cover - unused here
        return None


async def test_final_log_includes_event_loop() -> None:
    """Final crawl statistics log should include the event loop in use."""

    class NoopSpider(Spider):
        name = "noop"
        start_urls: tuple[str, ...] = ()

        async def parse(
            self, response: Response
        ) -> CallbackOutput:  # pragma: no cover - not invoked
            return None

    engine = Engine(NoopSpider(), concurrency=0)
    spy_logger = _SpyLogger()
    engine.logger = spy_logger

    await engine.run()

    final_logs = [
        ctx for msg, ctx in spy_logger.info_calls if msg == "Final crawl statistics"
    ]
    assert final_logs, "Expected final crawl statistics log entry"
    assert final_logs[-1].get("event_loop") in ["asyncio", "uvloop"]
