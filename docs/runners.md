# Runners

Runners are convenience helpers that build an `Engine` and start the crawl. See [src/silkworm/runner.py](../src/silkworm/runner.py).

## Async Entry Point: `crawl`
`crawl` is an async helper that instantiates the spider and runs the engine.

```python
from silkworm import crawl

await crawl(MySpider, concurrency=16, request_timeout=10)
```

## Sync Entry Point: `run_spider`
`run_spider` wraps `crawl` with `asyncio.run`.

```python
from silkworm import run_spider

run_spider(MySpider, concurrency=16, request_timeout=10)
```

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

> **Requires**: `pip install silkworm-rs[trio]`

## Engine Direct Usage
If you want to manage the lifecycle directly:

```python
from silkworm.engine import Engine
from silkworm.spiders import Spider

spider = Spider(name="custom")
engine = Engine(spider, concurrency=4)
# await engine.run()
```

Engine details: [src/silkworm/engine.py](../src/silkworm/engine.py)
