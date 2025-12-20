# Logging and Stats

Silkworm uses **logly** for structured logging and emits crawl statistics from the engine.

## Logger Basics
`get_logger` returns a shared, configured logly logger. See [src/silkworm/logging.py](../src/silkworm/logging.py).

```python
from silkworm.logging import get_logger

logger = get_logger(component="MySpider", spider="quotes")
logger.info("Started")
```

### Environment Controls
- **`SILKWORM_LOG_LEVEL`**: Sets the minimum log level (e.g., `DEBUG`, `INFO`).

## Spider Logger Injection
You can pass a logger or a context dict into the spider constructor. See [src/silkworm/spiders.py](../src/silkworm/spiders.py).

```python
run_spider(MySpider, logger={"component": "QuotesSpider", "env": "dev"})
```

The `Spider.log` property always returns a valid logger (creating one if needed).

## Crawl Statistics
The engine can emit periodic stats and always logs a final summary. See [src/silkworm/engine.py](../src/silkworm/engine.py).

Example:

```python
run_spider(MySpider, log_stats_interval=10)
```

Stats include:
- `requests_sent`, `responses_received`, `items_scraped`, `errors`
- `queue_size`, `seen_requests`, `requests_per_second`
- `memory_mb`, `elapsed_seconds`

## Example Scripts
- Logger configuration: [examples/logger_configuration_demo.py](../examples/logger_configuration_demo.py)
- Hybrid console + JSON logs: [examples/hybrid_logger_demo.py](../examples/hybrid_logger_demo.py)
