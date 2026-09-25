# Runners

Runners are convenience helpers that build an `Engine` and start the crawl. See [src/silkworm/runner.py](../src/silkworm/runner.py).

## Spiders and Engine Options
Every runner takes a spider and the same keyword **engine options**; the runners differ only in the event loop they use.

- **Spider**: pass a spider class to use its no-argument constructor, or an instance when the spider takes arguments. Constructor arguments then go to the spider directly, so type checkers verify them:

  ```python
  run_spider(QuotesSpider, concurrency=16)
  run_spider(SitemapSpider(sitemap_url=url, max_pages=5), concurrency=16)
  ```

- **Engine options** are the keyword parameters of `Engine`, described by the `EngineOptions` typed dict (`from silkworm import EngineOptions`): `concurrency`, `max_pending_requests`, `emulation`, `request_timeout`, `html_max_size_bytes`, `request_middlewares`, `response_middlewares`, `item_pipelines`, `log_stats_interval`, `keep_alive`, `http_client`, `engine_logger` and `dedup_key`. Omitted options use the `Engine` defaults. To share options between runs, build them once:

  ```python
  from silkworm import EngineOptions, run_spider

  options: EngineOptions = {"concurrency": 32, "request_timeout": 10, "keep_alive": True}
  run_spider(QuotesSpider, **options)
  ```

`emulation` selects the browser profile `wreq` impersonates (`Emulation.Firefox139` by default); pass `emulation=None` to disable it.

> **Upgrading:** runners no longer forward unknown keyword arguments to the spider constructor. Replace `run_spider(MySpider, pages=3, concurrency=8)` with `run_spider(MySpider(pages=3), concurrency=8)`.

## Async Entry Point: `crawl`
`crawl` is an async helper that runs the spider on the current event loop.

```python
from silkworm import crawl

await crawl(MySpider, concurrency=16, request_timeout=10)
```

## Sync Entry Point: `run_spider`
`run_spider` wraps `crawl` with `asyncio.run`. Pass `loop_factory=` to run on a custom asyncio event loop.

```python
from silkworm import run_spider

run_spider(MySpider, concurrency=16, request_timeout=10)
```

## rsloop
`run_spider_rsloop` installs rsloop and then runs the spider.

```python
from silkworm import run_spider_rsloop

run_spider_rsloop(MySpider, concurrency=32)
```

> **Requires**: `pip install silkworm-rs[rsloop]`

## uvloop (Unix)
`run_spider_uvloop` installs uvloop and then runs the spider.

```python
from silkworm import run_spider_uvloop

run_spider_uvloop(MySpider, concurrency=32)
```

> **Requires**: `pip install silkworm-rs[uvloop]`

## winloop (Windows)
`run_spider_winloop` installs winloop on Windows.

```python
from silkworm import run_spider_winloop

run_spider_winloop(MySpider, concurrency=32)
```

> **Requires**: `pip install silkworm-rs[winloop]`

## Trio
`run_spider_trio` uses trio + trio-asyncio for those who prefer trio semantics.

```python
from silkworm import run_spider_trio

run_spider_trio(MySpider, concurrency=16)
```

> **Requires**: Python 3.13 and `pip install silkworm-rs[trio]`. trio-asyncio
> 0.16 is not compatible with Python 3.14 or newer.

## Engine Direct Usage
If you want to manage the lifecycle directly:

```python
from silkworm.engine import Engine
from silkworm import Response, Spider

class CustomSpider(Spider):
    start_urls = ("https://example.com",)

    async def parse(self, response: Response):
        return None

spider = CustomSpider(name="custom")
engine = Engine(spider, concurrency=4)
# await engine.run()
```

Engine details: [src/silkworm/engine.py](../src/silkworm/engine.py)
