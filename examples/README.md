# Silkworm examples

Every file here is a small, runnable program with a comment at the top that
explains what it does and how to run it. Scraped data is written to the `data/`
folder, usually as JSON Lines (`.jl`): one JSON object per line.

Run examples from the project root, for example:

```bash
python examples/quotes_spider.py
```

## Key ideas in 30 seconds

- **Spider**: a class that says where to start (`start_urls`) and how to read a
  page (`parse()`).
- **`yield {...}`** in `parse()` produces an *item* (scraped data).
- **`yield response.follow(href, callback=...)`** asks Silkworm to download
  another page (for example the next page).
- **Middlewares** change requests before they are sent, or responses after they
  arrive (retry, user agent, delays, cookies...).
- **Pipelines** receive every item, for example to save it to a file or a database.

## Suggested learning order

### 1. Basics

| File | What you learn |
| --- | --- |
| `quotes_spider.py` | **Start here.** Selectors, items, pagination, middlewares, pipelines. |
| `quotes_spider_xpath.py` | The same spider with XPath instead of CSS selectors. |
| `declarative_quotes_spider.py` | Describe fields with `Item`, `Text`, `Attr` instead of writing selector code. |
| `callback_pipeline_demo.py` | Process items with your own functions. |
| `export_formats_demo.py` | Save to JSON Lines, XML, CSV (and MsgPack) at the same time. |

### 2. Real websites

| File | What you learn |
| --- | --- |
| `hackernews_spider.py` | Combine data from neighbouring rows, polite delays, `--pages` option. |
| `lobsters_spider.py` | Optional fields, regular expressions, uvloop, retry on 429. |
| `start_urls_from_file_spider.py` | Read URLs from a text file in `start_requests()`, use `meta`. |
| `url_titles_spider.py` | Fetch titles for many URLs from a JSONL file, choose an event loop. |
| `sitemap_spider.py` | Parse `sitemap.xml`, two callbacks, collect SEO/Open Graph tags. |

### 3. Going further

| File | What you learn |
| --- | --- |
| `exception_handling_spider.py` | Retry in `process_exception()`, handle failures in an `errback`. |
| `cookie_reuse_spiders.py` | Save cookies from one crawl and reuse them in another. |
| `runtime_stats_quotes_spider.py` | Add your own numbers to the crawl statistics log. |
| `logger_configuration_demo.py` | Three ways to give a spider a logger. |
| `hybrid_logger_demo.py` | Log as text on screen and as JSON in a file. |
| `logging_controls_demo.py` | Quieter logs and hiding secrets (tokens) from logs. |
| `quotes_spider_trio.py` | Run a spider on Trio instead of asyncio. |
| `quotes_spider_winloop.py` | Run a spider on winloop (Windows). |

### 4. Extra services and browsers (need extra setup)

| File | Needs |
| --- | --- |
| `taskiq_quotes_spider.py` | `pip install "silkworm-rs[taskiq]"` |
| `request_response_stream_spider.py` | A collector URL (e.g. from webhook.site) |
| `cloudflare_crawl_spider.py` | A Cloudflare account ID and API token |
| `lightpanda_simple.py` | `pip install "silkworm-rs[cdp]"` and a running Lightpanda/Chromium |
| `lightpanda_spider.py` | Same as above |
| `servo_spider.py` | `pip install servofetch` |

## Tips

- See more logs: `SILKWORM_LOG_LEVEL=DEBUG python examples/quotes_spider.py`
- Most examples with options support `--help`, e.g.
  `python examples/hackernews_spider.py --help`
- Be polite to websites: keep `DelayMiddleware` and low page limits when you
  experiment.
