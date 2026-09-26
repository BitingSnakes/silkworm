# Logging and Stats

Silkworm uses Python's standard `logging` module through a structured compatibility
adapter and emits crawl statistics from the engine.

## Logger Basics
`get_logger` returns a shared, configured logger adapter. It supports bound context
and structured keyword fields. See [src/silkworm/logging.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/logging.py).

```python
from silkworm.logging import get_logger

logger = get_logger(component="MySpider", spider="quotes")
logger.info("Started", url="https://example.com")
```

The returned object satisfies the `Logger` protocol (`from silkworm.types import Logger`):
`debug`, `info`, `warning`, `error`, and `exception` take a message plus keyword
context fields, and `bind(**context)` returns a logger with extra bound context.

### Environment Controls
- **`SILKWORM_LOG_LEVEL`**: Sets the minimum log level (e.g., `DEBUG`, `INFO`). It is read the first time a logger is requested; the default is `INFO`.

### Log Levels
`LogLevel` accepts `"TRACE"`, `"DEBUG"`, `"INFO"`, `"SUCCESS"`, `"WARNING"`, `"ERROR"`,
`"CRITICAL"`, and `"FAIL"` (plus the aliases `"WARN"`, `"ERR"`, and `"FATAL"`).
`None` means "do not log" wherever Silkworm accepts a configurable level.

- **`log_at_level(logger, level, message, **context)`**: Emit a message at a caller-selected level; `level=None` suppresses it.
- **`complete_logs()`**: Flush configured log handlers, for example before a process exits.

## Spider Logger Injection
You can pass a logger or a context dict into the spider constructor. See [src/silkworm/spiders.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/spiders.py).

```python
run_spider(MySpider(logger={"component": "QuotesSpider", "env": "dev"}))
```

The `Spider.log` property always returns a valid logger (creating one if needed).

## Engine Log Controls
`EngineLogger` controls how the engine reports per-request and per-item events.
Pass an instance as `engine_logger` to `run_spider`, `crawl`, or `Engine`:

```python
from silkworm import EngineLogger, run_spider

run_spider(
    MySpider,
    engine_logger=EngineLogger(
        fetched_response_level="DEBUG",  # default "INFO"
        fetching_request_level=None,  # default "DEBUG"; None suppresses the event
        item_pipeline_level=None,  # default "DEBUG"
        retry_request_level="INFO",  # default "DEBUG"
        include_request_url=False,  # omit URLs, e.g. when they contain tokens
    ),
)
```

Each event is a method you can override in a subclass to redact or reshape it:
`fetching_request`, `fetched_response`, `retrying_request`, and
`running_item_pipeline`.

```python
from typing import override

from silkworm import EngineLogger, Request, Response, Spider
from silkworm.types import Logger


class RedactedEngineLogger(EngineLogger):
    @override
    def fetched_response(
        self,
        logger: Logger,
        request: Request,
        response: Response,
        spider: Spider,
    ) -> None:
        logger.debug("Fetched response", status=response.status, spider=spider.name)
```

### Pipeline Log Levels
Per-item pipeline messages can be tuned per pipeline:

- `CallbackPipeline(..., log_level=...)` and `JsonLinesPipeline(..., log_level=...)` accept a `LogLevel` directly.
- `LoggedPipeline(pipeline, log_level=...)` wraps any other pipeline and sets its per-item log level; use `log_level=None` to silence it.

```python
from silkworm.pipelines import CSVPipeline, LoggedPipeline

item_pipelines = [LoggedPipeline(CSVPipeline("data/items.csv"), log_level=None)]
```

## Crawl Statistics
The engine can emit periodic stats and always logs a final summary. See [src/silkworm/engine.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/engine.py).

```python
run_spider(MySpider, log_stats_interval=10)
```

Stats include:
- `requests_sent`, `responses_received`, `items_scraped`, `errors`
- `queue_size`, `seen_requests`, `requests_per_second`
- `memory_mb`, `elapsed_seconds`

### Custom Spider Stats
Every spider has a `stats_payload` dict whose entries are merged into the periodic
and final summaries. Update it from callbacks with JSON-compatible values:

```python
class QuotesSpider(Spider):
    async def parse(self, response: Response):
        quotes = await response.select(".quote")
        self.stats_payload["quotes_seen"] = (
            self.stats_payload.get("quotes_seen", 0) + len(quotes)
        )
        ...
```

Keys starting with `_` and names reserved by the engine (the stats listed above,
plus `spider` and `event_loop`) are rejected with `KeyError`.

## Example Scripts
- Logger configuration: [examples/logger_configuration_demo.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/logger_configuration_demo.py)
- Hybrid console + JSON logs: [examples/hybrid_logger_demo.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/hybrid_logger_demo.py)
- Engine and pipeline log controls: [examples/logging_controls_demo.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/logging_controls_demo.py)
- Custom spider stats: [examples/runtime_stats_quotes_spider.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/runtime_stats_quotes_spider.py)
