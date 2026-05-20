# Engine and HTTP Client

Silkworm's **Engine** orchestrates crawl execution, while **HttpClient** performs HTTP requests using wreq by default.

## Engine
Engine runs the request queue, applies middlewares, invokes callbacks, and sends items through pipelines. See [src/silkworm/engine.py](../src/silkworm/engine.py).

Key behaviors:
- **Concurrency**: worker pool sized by positive `concurrency`.
- **Backpressure**: queue size defaults to `concurrency * 10` (override with positive `max_pending_requests`).
- **Priority**: higher `Request.priority` values are dequeued first; equal priorities keep FIFO order.
- **Deduplication**: request keys are cached unless `dont_filter=True`; the default key is `Request.url`.
- **Middleware flow**: request middlewares -> HTTP fetch -> response middlewares -> callbacks.
- **Pipeline flow**: each item passes through all pipelines in order.
- **Stats**: requests sent, responses received, items scraped, errors, queue size, memory, throughput.

Common Engine options (also exposed by `run_spider` and `crawl` in [src/silkworm/runner.py](../src/silkworm/runner.py)):
- **`concurrency`**: max concurrent requests; must be positive.
- **`max_pending_requests`**: queue bound for backpressure; must be positive when provided.
- **`request_timeout`**: per-request timeout (seconds or `timedelta`).
- **`html_max_size_bytes`**: HTML parsing size limit for selectors.
- **`log_stats_interval`**: periodic stats logging interval (seconds).
- **`keep_alive`**: reuse HTTP connections when supported.
- **`http_client`**: optional client instance to use instead of the default wreq-backed `HttpClient`.
- **`dedup_key`**: optional `Callable[[Request], str]` for request deduplication; defaults to `Request.url`.
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

### Callback Normalization
Engine accepts a wide range of callback outputs (single item, iterable, async iterable, awaitable). Any non-iterable value is treated as a single item to avoid confusing TypeErrors.

## HttpClient
HttpClient wraps wreq and is responsible for request serialization, redirects, and HTML detection. See [src/silkworm/http.py](../src/silkworm/http.py).

Core features:
- **Browser emulation**: `emulation=Emulation.Firefox139` by default.
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
See [src/silkworm/response.py](../src/silkworm/response.py).

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

Per-request render options are passed through `Request.meta`: `servo_javascript`, `servo_settle_ms`, `servo_user_agent`, `servo_screenshot`, and `servo_full_page`.

## CDP Rendering
For one-off rendered fetches through a CDP-compatible browser such as Lightpanda, Chrome, or Chromium, use `fetch_html_cdp` from [src/silkworm/api.py](../src/silkworm/api.py). For lower-level browser control, `CDPClient` is available when the `cdp` extra is installed.

```bash
pip install "silkworm-rs[cdp]"
```
