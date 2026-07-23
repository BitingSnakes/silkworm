from __future__ import annotations

from pathlib import Path
from typing import cast

from silkworm._types import JSONValue
from silkworm.engine import Engine, EngineLogger
from silkworm.logging import LogLevel, log_at_level
from silkworm.pipelines import JsonLinesPipeline, LoggedPipeline
from silkworm.request import CallbackOutput, Request
from silkworm.response import Response
from silkworm.spiders import Spider


class _SpyLogger:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, object]]] = []

    def info(self, message: str, **context: object) -> None:
        self.calls.append(("info", message, context))

    def debug(self, message: str, **context: object) -> None:
        self.calls.append(("debug", message, context))

    def warning(self, message: str, **context: object) -> None:
        self.calls.append(("warning", message, context))

    def error(self, message: str, **context: object) -> None:
        self.calls.append(("error", message, context))

    def configure(
        self,
        *,
        handlers: list[dict[str, object]] | None = None,
    ) -> None:
        return None

    def bind(self, **context: object) -> "_SpyLogger":
        return self

    def complete(self) -> None:
        return None


class _NoopSpider(Spider):
    name = "noop"
    start_urls: tuple[str, ...] = ()

    async def parse(self, response: Response) -> CallbackOutput:
        return None


class _LoggingPipeline:
    def __init__(self, logger: _SpyLogger) -> None:
        self.logger = logger

    async def open(self, spider: Spider) -> None:
        return None

    async def close(self, spider: Spider) -> None:
        return None

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        log_level = cast("LogLevel", getattr(self, "log_level", "DEBUG"))
        log_at_level(self.logger, log_level, "Processed item", spider=spider.name)
        return item


async def test_logged_pipeline_can_suppress_per_item_logs() -> None:
    spider = _NoopSpider()
    engine_logger = EngineLogger(item_pipeline_level="DEBUG")
    engine = Engine(
        spider,
        engine_logger=engine_logger,
        item_pipelines=[LoggedPipeline(_LoggingPipeline(_SpyLogger()), log_level=None)],
    )
    engine_logger_spy = _SpyLogger()
    engine.logger = engine_logger_spy

    await engine._process_item({"id": 1})

    assert engine_logger_spy.calls == []
    wrapped = engine.item_pipelines[0]
    assert isinstance(wrapped, LoggedPipeline)
    logging_pipeline = cast("_LoggingPipeline", wrapped.pipeline)
    assert logging_pipeline.logger.calls == []


async def test_json_lines_pipeline_accepts_log_level_none(
    tmp_path: Path,
) -> None:
    spider = _NoopSpider()
    pipeline = JsonLinesPipeline(tmp_path / "items.jl", log_level=None)
    pipeline_logger_spy = _SpyLogger()
    pipeline.logger = pipeline_logger_spy

    await pipeline.open(spider)
    await pipeline.process_item({"id": 1}, spider)
    await pipeline.close(spider)

    messages = [message for _, message, _ in pipeline_logger_spy.calls]
    assert "Wrote item to JSONL" not in messages
    assert (tmp_path / "items.jl").read_text(encoding="utf-8") == '{"id": 1}\n'


def test_engine_logger_can_demote_and_redact_fetched_response() -> None:
    spider = _NoopSpider()
    logger = _SpyLogger()
    request = Request(url="https://example.com/api?token=secret")
    response = Response(
        url=request.url, status=200, headers={}, body=b"", request=request
    )
    engine_logger = EngineLogger(
        fetched_response_level="DEBUG",
        include_request_url=False,
    )

    engine_logger.fetched_response(logger, request, response, spider)

    assert logger.calls == [
        ("debug", "Fetched response", {"status": 200, "spider": "noop"})
    ]


def test_engine_logger_accepts_none_for_fetched_response_level() -> None:
    spider = _NoopSpider()
    logger = _SpyLogger()
    request = Request(url="https://example.com/api?token=secret")
    response = Response(
        url=request.url, status=200, headers={}, body=b"", request=request
    )
    engine_logger = EngineLogger(fetched_response_level=None)

    engine_logger.fetched_response(logger, request, response, spider)

    assert logger.calls == []
