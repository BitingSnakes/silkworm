# Examples

All examples live under [examples/](https://github.com/BitingSnakes/silkworm/tree/main/examples). This page lists each example, what it demonstrates, and a typical command.

| Example | Focus | Command |
| --- | --- | --- |
| [examples/quotes_spider.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/quotes_spider.py) | Basic spider, validation, JSONL output | `python examples/quotes_spider.py` |
| [examples/quotes_spider_xpath.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/quotes_spider_xpath.py) | XPath selectors | `python examples/quotes_spider_xpath.py` |
| [examples/declarative_quotes_spider.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/declarative_quotes_spider.py) | Declarative `Item` extraction with pagination | `python examples/declarative_quotes_spider.py` |
| [examples/quotes_spider_trio.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/quotes_spider_trio.py) | Trio runner | `python examples/quotes_spider_trio.py` |
| [examples/quotes_spider_winloop.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/quotes_spider_winloop.py) | winloop runner | `python examples/quotes_spider_winloop.py` |
| [examples/hackernews_spider.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/hackernews_spider.py) | Pagination, delays, retries | `python examples/hackernews_spider.py --pages 5` |
| [examples/lobsters_spider.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/lobsters_spider.py) | uvloop runner, pagination | `python examples/lobsters_spider.py --pages 2` |
| [examples/start_urls_from_file_spider.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/start_urls_from_file_spider.py) | Text file input, custom start_requests | `python examples/start_urls_from_file_spider.py --urls-file data/start_urls.txt --output data/start_urls_from_file.jl` |
| [examples/url_titles_spider.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/url_titles_spider.py) | JSONL input, custom start_requests | `python examples/url_titles_spider.py --urls-file data/url_titles.jl --output data/titles.jl` |
| [examples/exception_handling_spider.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/exception_handling_spider.py) | Exception middleware and request errbacks | `python examples/exception_handling_spider.py` |
| [examples/cookie_reuse_spiders.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/cookie_reuse_spiders.py) | Cookie capture, save/load, and reuse across two spider runs | `python examples/cookie_reuse_spiders.py` |
| [examples/export_formats_demo.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/export_formats_demo.py) | JSONL, XML, CSV (and MsgPack if available) | `python examples/export_formats_demo.py --pages 2` |
| [examples/callback_pipeline_demo.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/callback_pipeline_demo.py) | CallbackPipeline chaining | `python examples/callback_pipeline_demo.py` |
| [examples/taskiq_quotes_spider.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/taskiq_quotes_spider.py) | TaskiqPipeline queue output | `python examples/taskiq_quotes_spider.py --pages 2` |
| [examples/sitemap_spider.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/sitemap_spider.py) | XML sitemap parsing, meta tags | `python examples/sitemap_spider.py --sitemap-url https://example.com/sitemap.xml --pages 50` |
| [examples/runtime_stats_quotes_spider.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/runtime_stats_quotes_spider.py) | Runtime stats payload updates | `python examples/runtime_stats_quotes_spider.py` |
| [examples/logging_controls_demo.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/logging_controls_demo.py) | Engine and pipeline logging controls | `python examples/logging_controls_demo.py --mode quiet` |
| [examples/logger_configuration_demo.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/logger_configuration_demo.py) | Logger injection patterns | `python examples/logger_configuration_demo.py` |
| [examples/hybrid_logger_demo.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/hybrid_logger_demo.py) | Hybrid console + JSON logs | `python examples/hybrid_logger_demo.py` |
| [examples/request_response_stream_spider.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/request_response_stream_spider.py) | Request/response telemetry streaming | `python examples/request_response_stream_spider.py --collector-url https://collector.example.com/events` |
| [examples/cloudflare_crawl_spider.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/cloudflare_crawl_spider.py) | Cloudflare Browser Rendering crawl jobs | `CLOUDFLARE_ACCOUNT_ID=... CLOUDFLARE_API_TOKEN=... python examples/cloudflare_crawl_spider.py https://example.com --limit 10` |
| [examples/lightpanda_simple.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/lightpanda_simple.py) | CDP one-off rendered HTML fetch | `python examples/lightpanda_simple.py` |
| [examples/lightpanda_spider.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/lightpanda_spider.py) | CDP spider flow with Lightpanda/Chrome endpoint | `python examples/lightpanda_spider.py` |
| [examples/servo_spider.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/servo_spider.py) | Servo-rendered spider using `ServoFetchClient` | `python examples/servo_spider.py` |

> **Tip:** Some examples require optional extras (rsloop, trio, winloop, taskiq, cdp). Servo examples require a compatible `servofetch` wheel from the servofetch releases rather than a package extra.
