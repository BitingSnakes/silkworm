# Examples

All examples live under [examples/](../examples). This page lists each example, what it demonstrates, and a typical command.

| Example | Focus | Command |
| --- | --- | --- |
| [examples/quotes_spider.py](../examples/quotes_spider.py) | Basic spider, validation, JSONL output | `python examples/quotes_spider.py` |
| [examples/quotes_spider_xpath.py](../examples/quotes_spider_xpath.py) | XPath selectors | `python examples/quotes_spider_xpath.py` |
| [examples/quotes_spider_trio.py](../examples/quotes_spider_trio.py) | Trio runner | `python examples/quotes_spider_trio.py` |
| [examples/quotes_spider_winloop.py](../examples/quotes_spider_winloop.py) | winloop runner | `python examples/quotes_spider_winloop.py` |
| [examples/hackernews_spider.py](../examples/hackernews_spider.py) | Pagination, delays, retries | `python examples/hackernews_spider.py --pages 5` |
| [examples/lobsters_spider.py](../examples/lobsters_spider.py) | uvloop runner, pagination | `python examples/lobsters_spider.py --pages 2` |
| [examples/url_titles_spider.py](../examples/url_titles_spider.py) | JSONL input, custom start_requests | `python examples/url_titles_spider.py --urls-file data/url_titles.jl --output data/titles.jl` |
| [examples/export_formats_demo.py](../examples/export_formats_demo.py) | JSONL, XML, CSV (and MsgPack if available) | `python examples/export_formats_demo.py --pages 2` |
| [examples/callback_pipeline_demo.py](../examples/callback_pipeline_demo.py) | CallbackPipeline chaining | `python examples/callback_pipeline_demo.py` |
| [examples/taskiq_quotes_spider.py](../examples/taskiq_quotes_spider.py) | TaskiqPipeline queue output | `python examples/taskiq_quotes_spider.py --pages 2` |
| [examples/sitemap_spider.py](../examples/sitemap_spider.py) | XML sitemap parsing, meta tags | `python examples/sitemap_spider.py --sitemap-url https://example.com/sitemap.xml --pages 50` |
| [examples/logger_configuration_demo.py](../examples/logger_configuration_demo.py) | Logger injection patterns | `python examples/logger_configuration_demo.py` |
| [examples/hybrid_logger_demo.py](../examples/hybrid_logger_demo.py) | Hybrid console + JSON logs | `python examples/hybrid_logger_demo.py` |

> **Tip:** Some examples require optional extras (trio, winloop, taskiq). Install them via `pip install "silkworm-rs[trio]"` or similar.
