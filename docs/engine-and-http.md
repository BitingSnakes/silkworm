# Engine and HTTP Client

Silkworm's **Engine** orchestrates crawl execution, while **HttpClient** performs HTTP requests using wreq by default.

## Engine
Engine runs the request queue, applies middlewares, invokes callbacks, and sends items through pipelines. See [src/silkworm/engine.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/engine.py).

Key behaviors:
- **Concurrency**: worker pool sized by positive `concurrency`.
- **Backpressure**: queue size defaults to `concurrency * 10` (override with positive `max_pending_requests`).
- **Priority**: higher `Request.priority` values are dequeued first; equal priorities keep FIFO order.
- **Deduplication**: request keys are cached unless `dont_filter=True`; the default key is `Request.url` (`default_dedup_key`).
- **Middleware flow**: request middlewares -> HTTP fetch -> response middlewares -> callbacks.
- **Pipeline flow**: each item passes through all pipelines in order.
- **Stats**: requests sent, responses received, items scraped, errors, queue size, memory, throughput.

Common Engine options (also exposed by `run_spider` and `crawl` in [src/silkworm/runner.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/runner.py)):
- **`concurrency`**: max concurrent requests; must be positive.
- **`max_pending_requests`**: queue bound for backpressure; must be positive when provided.
- **`request_timeout`**: per-request timeout (seconds or `timedelta`).
- **`html_max_size_bytes`**: HTML parsing size limit for selectors.
- **`log_stats_interval`**: periodic stats logging interval (seconds).
- **`keep_alive`**: reuse HTTP connections when supported.
- **`http_client`**: optional client instance to use instead of the default wreq-backed `HttpClient`.
- **`dedup_key`**: optional `DedupKey` (`Callable[[Request], str]`) for request deduplication; defaults to `default_dedup_key`, which returns `Request.url`.
- **`emulation`**: browser profile impersonated by the default client (`Emulation.Firefox139`); `None` disables it.
- **`engine_logger`**: an `EngineLogger` controlling per-event log levels (see [Logging and Stats](logging-and-stats.md#engine-log-controls)).
- **`request_middlewares`**, **`response_middlewares`**, **`item_pipelines`**: plug-ins executed by the engine.

```python
from silkworm.engine import Engine
from silkworm import Response, Spider

class DemoSpider(Spider):
    start_urls = ("https://example.com",)

    async def parse(self, response: Response):
        return None

spider = DemoSpider(name="demo")
engine = Engine(spider, concurrency=8, log_stats_interval=10)
# await engine.run()
```

Use a custom deduplication key when URL-only deduplication is too coarse:

```python
from urllib.parse import urlencode

from silkworm import Request, run_spider


def dedup_with_params(req: Request) -> str:
    return f"{req.url}?{urlencode(req.params, doseq=True)}"


run_spider(MySpider, dedup_key=dedup_with_params)
```

### Lifecycle
`Engine.run()` calls `open_spider()`, which opens middlewares (each instance once, even when registered in both lists), then the spider's `open()`, then pipelines in order, and enqueues `start_requests()`. When the queue drains, `close_spider()` closes pipelines, the spider, and middlewares in reverse order, and the HTTP client is closed. Middlewares and pipelines may implement optional async `open(spider)` / `close(spider)` hooks.

### Callback Normalization
Engine accepts a wide range of callback outputs (single item, iterable, async iterable, awaitable). Any non-iterable value is treated as a single item to avoid confusing TypeErrors.

## HttpClient
HttpClient wraps wreq and is responsible for request serialization, redirects, and HTML detection. See [src/silkworm/http.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/http.py).

Constructor options: `concurrency`, `emulation`, `default_headers` (merged below per-request headers), `timeout`, `html_max_size_bytes`, `follow_redirects` (default `True`), `max_redirects` (default `10`), `keep_alive`, and `**client_kwargs` forwarded to `wreq.Client`. The engine builds one for you; pass `http_client=HttpClient(...)` to customize it.

Core features:
- **Browser emulation**: `emulation=Emulation.Firefox139` by default, for both `HttpClient` and the client `Engine` creates; pass `emulation=None` to disable it.
- **Timeouts**: per-request or global (seconds or `timedelta`).
- **Redirects**: automatic follow with loop detection and max redirect cap.
- **Keep-alive**: optional connection reuse when supported by the underlying client.
- **Proxy support**: uses `request.meta["proxy"]`.
- **Query merging**: `Request.params` are merged with existing query strings.
- **HTML detection**: returns `HTMLResponse` when content-type/sniffing indicates HTML.

### Redirect Behavior
The client follows redirects for 301/302/303/307/308 responses with `Location`.
For 301/302/303, non-GET/HEAD methods are switched to GET (body cleared). It also updates `request.meta["redirect_times"]`.

```python
from silkworm import Request
from silkworm.http import HttpClient

client = HttpClient(max_redirects=5)
resp = await client.fetch(Request(url="https://example.com"))
print(resp.url, resp.status)
```

### HTML Detection
The client inspects content-type and a small body snippet to decide whether to return `HTMLResponse` or plain `Response`.

```python
from silkworm import Response, HTMLResponse

# In a callback, you may get HTMLResponse directly if content is HTML.
if isinstance(response, HTMLResponse):
    title = await response.select_first("title")
```

### Text Decoding
`Response.text` uses BOM, headers, and HTML meta tags before falling back to `charset-normalizer` when available.
See [src/silkworm/response.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/response.py).

### Mock Responses for Testing
Set `request.meta[MOCK_RESPONSE_META_KEY]` (from `silkworm.http`) to a mapping with `status`, `headers`, `body`, and optional `url` to have `HttpClient` return that response without touching the network. HTML bodies become `HTMLResponse`. This is handy for tests and offline demos:

```python
from silkworm import Request
from silkworm.http import MOCK_RESPONSE_META_KEY

Request(
    url="https://api.example.test/items",
    meta={
        MOCK_RESPONSE_META_KEY: {
            "status": 200,
            "headers": {"content-type": "application/json"},
            "body": '{"items": []}',
        }
    },
)
```

See [examples/logging_controls_demo.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/logging_controls_demo.py) for a complete spider built on mock responses.

## OnionLinkClient
`OnionLinkClient` is an optional client adapter for scraping Tor v3 onion services through [onionlink](https://github.com/RustedBytes/onionlink) instead of wreq. Install the OnionLink extra before using it:

```bash
pip install "silkworm-rs[onionlink]"
```

Then pass an instance to `run_spider`, `crawl`, or `Engine`:

```python
from silkworm import OnionLinkClient, Spider, run_spider


class OnionSpider(Spider):
    start_urls = ("http://exampleexampleexampleexampleexampleexampleexampleexampleexampleexample.onion/",)


run_spider(
    OnionSpider,
    http_client=OnionLinkClient(concurrency=4, timeout=30),
)
```

Constructor options: `concurrency`, `default_headers`, `timeout`, `html_max_size_bytes`, `follow_redirects`, `max_redirects`, `bootstrap` (Tor directory authority endpoint), `consensus_file` (optional consensus cache path), `verbose`, and `response_limit` (default 4 MiB).

`Request.params`, headers, body, JSON payloads, redirects, HTML detection, and `request.meta["redirect_times"]` work the same way as the default client. To override onionlink's per-response byte cap for one request, set `request.meta["onionlink_response_limit"]` to an integer byte limit.

## ServoFetchClient
`ServoFetchClient` is a client adapter for JavaScript-rendered pages through `servofetch`. The adapter is exported from `silkworm`, but `servofetch` is distributed as external wheels rather than a `pyproject.toml` extra.

```python
from silkworm import ServoFetchClient, Spider, run_spider


class RenderedSpider(Spider):
    start_urls = ("https://example.com",)


run_spider(
    RenderedSpider,
    http_client=ServoFetchClient(settle_ms=500),
)
```

Constructor options: `concurrency`, `timeout`, `settle_ms` (delay after load before capture), `user_agent`, `allow_private_addresses` (default `False`), `html_max_size_bytes`, and Tor settings for `.onion` pages (`onion_bootstrap`, `onion_consensus_file`, `onion_verbose`, `onion_response_limit`).

Per-request render options are passed through `Request.meta`: `servo_javascript`, `servo_settle_ms`, `servo_user_agent`, `servo_screenshot`, and `servo_full_page`. The keys are also exported from `silkworm.servo` as `SERVO_JAVASCRIPT_META_KEY`, `SERVO_SETTLE_MS_META_KEY`, `SERVO_USER_AGENT_META_KEY`, `SERVO_SCREENSHOT_META_KEY`, and `SERVO_FULL_PAGE_META_KEY`.

For a one-off render, `fetch_html_servo(url, timeout=None, settle_ms=0, user_agent=None, javascript=None, allow_private_addresses=False)` returns `(text, AsyncDocument)`.

## CDP Rendering
For one-off rendered fetches through a CDP-compatible browser such as Lightpanda, Chrome, or Chromium, use `fetch_html_cdp` from [src/silkworm/api.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/api.py). For lower-level browser control, `CDPClient` is available when the `cdp` extra is installed.

```bash
pip install "silkworm-rs[cdp]"
```

`fetch_html_cdp(url, ws_endpoint="ws://127.0.0.1:9222", timeout=None)` returns `(text, AsyncDocument)` for the rendered page.

To render every page of a crawl, connect a `CDPClient` and pass it as `http_client`. Options: `ws_endpoint`, `concurrency`, `timeout`, and `html_max_size_bytes`. Call `await client.connect()` before the crawl; the engine closes the client when the crawl ends.

```python
from silkworm import CDPClient, crawl


async def main() -> None:
    client = CDPClient(ws_endpoint="ws://127.0.0.1:9222", timeout=30.0)
    await client.connect()
    await crawl(MySpider, http_client=client)
```

CDP does not reliably expose navigation status, so rendered responses report status `200`; the final URL reflects redirects when the browser supports it. See [examples/lightpanda_spider.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/lightpanda_spider.py).
