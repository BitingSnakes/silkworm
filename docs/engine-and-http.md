# Engine and HTTP Client

Silkworm's **Engine** orchestrates crawl execution, while **HttpClient** performs HTTP requests using rnet.

## Engine
Engine runs the request queue, applies middlewares, invokes callbacks, and sends items through pipelines. See [src/silkworm/engine.py](../src/silkworm/engine.py).

Key behaviors:
- **Concurrency**: worker pool sized by `concurrency`.
- **Backpressure**: queue size defaults to `concurrency * 10` (override with `max_pending_requests`).
- **Deduplication**: request URLs are cached unless `dont_filter=True`.
- **Middleware flow**: request middlewares -> HTTP fetch -> response middlewares -> callbacks.
- **Pipeline flow**: each item passes through all pipelines in order.
- **Stats**: requests sent, responses received, items scraped, errors, queue size, memory, throughput.

Common Engine options (also exposed by `run_spider` and `crawl` in [src/silkworm/runner.py](../src/silkworm/runner.py)):
- **`concurrency`**: max concurrent requests.
- **`max_pending_requests`**: queue bound for backpressure.
- **`request_timeout`**: per-request timeout (seconds or `timedelta`).
- **`html_max_size_bytes`**: HTML parsing size limit for selectors.
- **`log_stats_interval`**: periodic stats logging interval (seconds).
- **`keep_alive`**: reuse HTTP connections when supported.
- **`request_middlewares`**, **`response_middlewares`**, **`item_pipelines`**: plug-ins executed by the engine.

```python
from silkworm.engine import Engine
from silkworm.spiders import Spider

spider = Spider(name="demo")
engine = Engine(spider, concurrency=8, log_stats_interval=10)
# await engine.run()
```

### Callback Normalization
Engine accepts a wide range of callback outputs (single item, iterable, async iterable, awaitable). Any non-iterable value is treated as a single item to avoid confusing TypeErrors.

## HttpClient
HttpClient wraps rnet and is responsible for request serialization, redirects, and HTML detection. See [src/silkworm/http.py](../src/silkworm/http.py).

Core features:
- **Browser emulation**: `emulation=Emulation.Firefox139` by default.
- **Timeouts**: per-request or global (seconds or `timedelta`).
- **Redirects**: automatic follow with loop detection and max redirect cap.
- **Keep-alive**: optional connection reuse when supported by the underlying client.
- **Proxy support**: uses `request.meta["proxy"]`.
- **Query merging**: `Request.params` are merged with existing query strings.
- **HTML detection**: returns `HTMLResponse` when content-type/sniffing indicates HTML.

### Redirect Behavior
If a response status is 301, 302, or 303, the client switches non-GET/HEAD methods to GET (body cleared) and updates `request.meta["redirect_times"]`.

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
