# Getting Started

## Requirements
- **Python**: 3.13+ (3.14 is supported experimentally).
- **Project metadata**: [pyproject.toml](../pyproject.toml)

## Installation

### From PyPI (pip)
```bash
pip install silkworm-rs
```

### From PyPI (uv)
```bash
uv pip install --prerelease=allow silkworm-rs
# or if using uv project management
uv add --prerelease=allow silkworm-rs
```

### From Source
```bash
uv venv --python python3.13
source .venv/bin/activate
uv pip install --prerelease=allow -e .
```

> **Note:** `--prerelease=allow` is required because some dependencies ship as prerelease versions.

## Optional Extras
Silkworm ships many integrations behind extras. Install only what you need.

```bash
pip install "silkworm-rs[uvloop,polars]"
```

| Extra | Purpose | Related Code |
| --- | --- | --- |
| `uvloop` | Faster event loop on Unix | [src/silkworm/runner.py](../src/silkworm/runner.py) |
| `winloop` | Faster event loop on Windows | [src/silkworm/runner.py](../src/silkworm/runner.py) |
| `trio` | Trio backend | [src/silkworm/runner.py](../src/silkworm/runner.py) |
| `msgpack` | MsgPack export | [src/silkworm/pipelines.py](../src/silkworm/pipelines.py) |
| `polars` | Parquet export | [src/silkworm/pipelines.py](../src/silkworm/pipelines.py) |
| `excel` | Excel export | [src/silkworm/pipelines.py](../src/silkworm/pipelines.py) |
| `yaml` | YAML export | [src/silkworm/pipelines.py](../src/silkworm/pipelines.py) |
| `avro` | Avro export | [src/silkworm/pipelines.py](../src/silkworm/pipelines.py) |
| `elasticsearch` | Elasticsearch export | [src/silkworm/pipelines.py](../src/silkworm/pipelines.py) |
| `mongodb` | MongoDB export | [src/silkworm/pipelines.py](../src/silkworm/pipelines.py) |
| `s3` | S3 export (OpenDAL) | [src/silkworm/pipelines.py](../src/silkworm/pipelines.py) |
| `vortex` | Vortex export | [src/silkworm/pipelines.py](../src/silkworm/pipelines.py) |
| `mysql` | MySQL export | [src/silkworm/pipelines.py](../src/silkworm/pipelines.py) |
| `postgresql` | PostgreSQL export | [src/silkworm/pipelines.py](../src/silkworm/pipelines.py) |
| `gsheets` | Google Sheets export | [src/silkworm/pipelines.py](../src/silkworm/pipelines.py) |
| `snowflake` | Snowflake export | [src/silkworm/pipelines.py](../src/silkworm/pipelines.py) |
| `ftp` | FTP export | [src/silkworm/pipelines.py](../src/silkworm/pipelines.py) |
| `sftp` | SFTP export | [src/silkworm/pipelines.py](../src/silkworm/pipelines.py) |
| `cassandra` | Cassandra export (not on Windows) | [src/silkworm/pipelines.py](../src/silkworm/pipelines.py) |
| `couchdb` | CouchDB export | [src/silkworm/pipelines.py](../src/silkworm/pipelines.py) |
| `dynamodb` | DynamoDB export | [src/silkworm/pipelines.py](../src/silkworm/pipelines.py) |
| `duckdb` | DuckDB export | [src/silkworm/pipelines.py](../src/silkworm/pipelines.py) |
| `taskiq` | Taskiq queue pipeline | [src/silkworm/pipelines.py](../src/silkworm/pipelines.py) |
| `memray` | Memory profiling | [justfile](../justfile) |

## Your First Spider
This is a minimal spider that extracts quotes and writes JSON Lines output. For a full version, see [examples/quotes_spider.py](../examples/quotes_spider.py).

```python
from silkworm import HTMLResponse, Response, Spider, run_spider
from silkworm.middlewares import RetryMiddleware, UserAgentMiddleware
from silkworm.pipelines import JsonLinesPipeline


class QuotesSpider(Spider):
    name = "quotes"
    start_urls = ("https://quotes.toscrape.com/",)

    async def parse(self, response: Response):
        if not isinstance(response, HTMLResponse):
            return

        html = response
        for el in await html.select(".quote"):
            text_el = await el.select_first(".text")
            author_el = await el.select_first(".author")
            if text_el is None or author_el is None:
                continue
            tags = await el.select(".tag")
            yield {
                "text": text_el.text,
                "author": author_el.text,
                "tags": [t.text for t in tags],
            }

        if next_link := await html.select_first("li.next > a"):
            yield html.follow(next_link.attr("href"), callback=self.parse)


run_spider(
    QuotesSpider,
    request_middlewares=[UserAgentMiddleware()],
    response_middlewares=[RetryMiddleware(max_times=3)],
    item_pipelines=[JsonLinesPipeline("data/quotes.jl")],
    concurrency=16,
    request_timeout=10,
    log_stats_interval=30,
)
```

## Running Examples
Examples are in [examples/](../examples). A few popular ones:

```bash
python examples/quotes_spider.py
python examples/quotes_spider_xpath.py
python examples/url_titles_spider.py --urls-file data/url_titles.jl --output data/titles.jl
```

See [Examples](examples.md) for a full list and what each one demonstrates.

## One-off HTML Fetch
For quick, standalone fetches, use `fetch_html` in [src/silkworm/api.py](../src/silkworm/api.py).

```python
import asyncio
from silkworm import fetch_html


async def main():
    text, doc = await fetch_html("https://example.com")
    title = doc.select_first("title")
    print(title.text if title else "no title")


asyncio.run(main())
```

## Development Workflow
Developer commands are defined in [justfile](../justfile) and [justfile-3.14t](../justfile-3.14t).

```bash
just help
just fmt
just lint
just typecheck
just test
```
