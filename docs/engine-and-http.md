# Engine and HTTP Client

Silkworm's **Engine** orchestrates crawl execution, while **HttpClient** performs HTTP requests using wreq by default.

## Engine
Engine runs the request queue, applies middlewares, invokes callbacks, and sends items through pipelines. See [src/silkworm/engine.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/engine.py).

Key behaviors:
- **Concurrency**: worker pool sized by positive `concurrency`.
- **Backpressure**: `max_pending_requests` (default `concurrency * 10`) bounds the queue; see [Queue Capacity and Deadlock Freedom](#queue-capacity-and-deadlock-freedom) for the exact guarantee.
- **Priority**: higher `Request.priority` values are dequeued first; equal priorities keep FIFO order.
- **Deduplication**: request keys are cached unless `dont_filter=True`; the default key is the request fingerprint (method, canonical URL with params, body; see `default_dedup_key`).
- **Scheduling filters**: `Spider.allowed_domains`, `max_depth`, and robots.txt (via `RobotsTxtMiddleware`) drop requests before they are fetched.
- **Stop limits and failure policy**: `max_requests`, `max_items`, `max_errors`, `max_duration`, `max_error_rate`, `min_items`, and `max_item_drop_rate`; see [Production Crawling](production.md).
- **Middleware flow**: request middlewares -> HTTP fetch -> response middlewares -> callbacks.
- **Pipeline flow**: each item passes through all pipelines in order.
- **Stats**: counters and per-status/domain/error-type breakdowns, returned as a `CrawlResult` from `run()` and optionally served as Prometheus metrics.
- **Graceful stop**: `engine.stop(reason)` finishes in-flight requests and closes pipelines; `close_reason` and `in_flight` expose the state.

Common Engine options (also exposed by `run_spider` and `crawl` in [src/silkworm/runner.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/runner.py)):
- **`concurrency`**: max concurrent requests; must be positive.
- **`max_pending_requests`**: queue capacity for backpressure (a hard bound for `start_requests()` and for a single producing callback, soft when several callbacks produce at once); must be positive when provided.
- **`request_timeout`**: per-request timeout (seconds or `timedelta`); defaults to 60 seconds, `None` disables it, and `Request.timeout` overrides it per request. It covers the whole exchange including the body download, restarts per redirect hop, and excludes waiting for a concurrency slot.
- **`html_max_size_bytes`**: HTML parsing size limit for selectors.
- **`max_response_size_bytes`**: largest downloaded body (default 50 MB; `None` for no limit).
- **`item_batch_size`**: emitted items per pipeline batch; defaults to `1`, which
  preserves immediate per-item processing.
- **`item_batch_wait`**: maximum wait for a partial item batch; defaults to 0.05
  seconds and applies only when batching is enabled.
- **`log_stats_interval`**: periodic stats logging interval (seconds).
- **`keep_alive`**: reuse HTTP connections when supported.
- **`http_client`**: optional client instance to use instead of the default wreq-backed `HttpClient`.
- **`dedup_key`**: optional `DedupKey` (`Callable[[Request], str]`) for request deduplication; defaults to `default_dedup_key`, which returns `request_fingerprint(request)`.
- **`concurrency_per_domain`**: maximum simultaneous fetches per host.
- **`max_depth`**, **`max_requests`**, **`max_items`**, **`max_errors`**, **`max_duration`**: stop limits.
- **`max_error_rate`**, **`min_items`**, **`max_item_drop_rate`**: failure policy; violations raise `CrawlFailedError`.
- **`job_dir`**: persist state to pause and resume crawls.
- **`http_cache`**: an `HttpCache` serving responses from disk.
- **`metrics_port`**, **`metrics_host`**: serve Prometheus metrics while crawling.
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
`Engine.run()` calls `open_spider()`, which opens middlewares (each instance once, even when registered in both lists), then the spider's `open()`, then pipelines in order, and runs `start_requests()`, whose `follow` calls enqueue the initial requests. When the queue drains, `close_spider()` closes pipelines, the spider, and middlewares in reverse order, and the HTTP client is closed. Middlewares and pipelines may implement optional async `open(spider)` / `close(spider)` hooks.

### Queue Capacity and Deadlock Freedom
Workers are the only consumers of the request queue, and callbacks run inside workers. A plain bounded queue would deadlock as soon as every worker waits to schedule a request into a full queue, so the engine enforces `max_pending_requests` itself:

- **`start_requests()`** runs outside the workers and always waits for space, so seeding never pushes the queue past `max_pending_requests`.
- **One callback at a time may wait.** A callback (or a task it spawned) that finds the queue full waits while it is the only waiting worker and at least one other worker exists. This throttles a single heavy producer, such as a sitemap callback scheduling thousands of pages, while the other workers drain the queue.
- **Other callbacks enqueue past the bound** and release the waiting worker to do the same. When pages keep producing more links than the workers consume, the queue can only grow; parking workers would idle them and keep their responses in memory without bounding the queue, so they keep crawling at full concurrency instead.
- **With `concurrency=1`, callbacks never wait**, because no other worker could make room.
- Errbacks and middleware retries follow the same rules because they are also scheduled from workers.

So the queue is hard-bounded for seeding and for a single producing callback, and may exceed `max_pending_requests` only when several callbacks produce requests at the same time. Workers can never deadlock on the queue. Priority ordering and deduplication are unaffected: dedup runs before any waiting, and all requests share one priority queue.

### Callback Execution
Every callback, errback, and `start_requests()` runs inside a crawl scope bound to the current task through a context variable. `emit` sends items directly to pipelines by default or to the bounded item queue when batching is enabled; `follow` sends requests to the deduplicating request queue. Both apply backpressure without deadlocking workers. Callback scopes drain pending item batches before returning. Callbacks must be `async` functions returning `None`; async generators, synchronous callables, and non-`None` return values raise `SpiderError` with migration hints. The scope closes when the callback returns, so detached tasks cannot report results after the response has been released. See [Core Concepts](core-concepts.md#reporting-results-emit-and-follow).

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
- **wreq runtime**: pass a `wreq.Runtime` through `HttpClient(runtime=...)` to choose worker count and work stealing. The runtime can be shared with other wreq clients.

wreq also provides `Emulation.Chrome154` and `Emulation.Firefox152`; pass either as `emulation=` to use the newer browser profile. Silkworm converts wreq's read-only `memoryview` body and header data to its existing `bytes` and `str` response fields.

To configure the runtime for a crawl:

```python
from __future__ import annotations

from typing import override

from wreq import Runtime

from silkworm import Response, Spider, run_spider
from silkworm.http import HttpClient


class ExampleSpider(Spider):
    name = "runtime-example"
    start_urls = ("https://example.com/",)

    @override
    async def parse(self, response: Response) -> None:
        await self.emit({"url": response.url, "status": response.status})


runtime = Runtime(workers=4, work_steal=False)
run_spider(ExampleSpider, http_client=HttpClient(concurrency=16, runtime=runtime))
```

`workers` sets the number of wreq runtime threads. `concurrency` separately limits requests in flight within Silkworm.

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
`Response.text` uses BOM, headers, and HTML meta tags before falling back to `charset-norm` when available.
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

To render every page of a crawl, connect a `CDPClient` and pass it as `http_client`. Options: `ws_endpoint`, `concurrency`, `timeout`, and `html_max_size_bytes`. A bare `ws://host:port` (or `http://host:port`) endpoint works with Lightpanda and Chrome/Chromium alike: if the browser refuses it, the client connects to the `webSocketDebuggerUrl` advertised at `/json/version`, keeping the configured host and port. Call `await client.connect()` before the crawl; the engine closes the client when the crawl ends.

```python
from silkworm import CDPClient, crawl


async def main() -> None:
    client = CDPClient(ws_endpoint="ws://127.0.0.1:9222", timeout=30.0)
    await client.connect()
    await crawl(MySpider, http_client=client)
```

CDP does not reliably expose navigation status, so rendered responses report status `200`; the final URL reflects redirects when the browser supports it. See [examples/lightpanda_spider.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/lightpanda_spider.py).
