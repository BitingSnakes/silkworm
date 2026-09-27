# Production Crawling

This page collects everything needed to run spiders unattended: knowing whether a
crawl succeeded, stopping it safely, being polite to sites, bounding memory,
resuming interrupted crawls, observing progress, and testing spiders offline.

A production-ready run typically combines several of these:

```python
from silkworm import run_spider
from silkworm.middlewares import (
    AutoThrottleMiddleware,
    RetryMiddleware,
    RobotsTxtMiddleware,
    UserAgentMiddleware,
)
from silkworm.pipelines import JsonLinesPipeline, ValidationPipeline

throttle = AutoThrottleMiddleware(start_delay=0.5, max_delay=30)

result = run_spider(
    ProductsSpider,
    request_middlewares=[UserAgentMiddleware(), RobotsTxtMiddleware(), throttle],
    response_middlewares=[throttle, RetryMiddleware(max_times=3)],
    item_pipelines=[ValidationPipeline(Product), JsonLinesPipeline("data/products.jl")],
    request_timeout=30,
    concurrency=16,
    concurrency_per_domain=4,
    max_depth=5,
    max_duration=3600,
    max_error_rate=0.05,
    min_items=100,
    max_item_drop_rate=0.1,
    job_dir="state/products",
    metrics_port=9410,
)
print(result.close_reason, result.items_scraped, result.error_rate)
```

The same options are available from the [command line](cli.md), environment
variables, and `Spider.custom_settings` (see [Settings](#settings)).

## Knowing whether a crawl succeeded

`Engine.run()`, `crawl()`, and every `run_spider*()` runner return a
{class}`~silkworm.CrawlResult`:

| Attribute | Meaning |
| --- | --- |
| `close_reason` | `"finished"` when the queue drained; `"shutdown"` after `engine.stop()` or a signal; a limit name such as `"max_items"`; or a `CloseSpider` reason. |
| `stats` | Final counters: `requests_sent`, `responses_received`, `items_scraped`, `items_dropped`, `errors`, `retries`, `dupe_filtered`, `offsite_filtered`, `depth_filtered`, `ignored_requests`, `dropped_requests`. |
| `labeled_stats` | Breakdowns: `responses_by_status`, `requests_by_domain`, `errors_by_type`, `items_dropped_by_reason`, `ignored_by_reason`. |
| `custom_stats` | A copy of the spider's `stats_payload`. |
| `error_rate`, `item_drop_rate` | Derived ratios. |
| `failures`, `ok` | Failure-policy violations (below). |

`errors` counts requests whose fetch or callback failed and were not recovered
by an exception middleware (retries do not count until they give up).

### Failure policy

A spider whose selectors stopped matching still "runs" successfully, so declare
what success means and the crawl raises
{class}`~silkworm.exceptions.CrawlFailedError` (with the full result in
`exc.result`) when it is violated:

- `max_error_rate`: fail when `errors / requests_sent` exceeds this fraction.
- `min_items`: fail when fewer items passed every pipeline.
- `max_item_drop_rate`: fail when pipelines dropped more than this share of items
  (items dropped only because `max_items` was reached do not count).

The policy is evaluated when the crawl ends on its own or hits a stop limit, but
not after a deliberate `stop()`/signal. The CLI exits with status `1` on a
violation, so cron jobs and CI catch broken spiders.

```python
from silkworm import CrawlFailedError, run_spider

try:
    run_spider(MySpider, max_error_rate=0.1, min_items=1)
except CrawlFailedError as exc:
    alert(exc.result.failures, exc.result.labeled_stats["errors_by_type"])
    raise
```

## Retries

`RetryMiddleware` retries retryable HTTP statuses (as a response middleware)
**and** transient transport failures: timeouts
({class}`~silkworm.exceptions.HttpTimeoutError`) and connection errors such as
refused, reset, or dropped connections
({class}`~silkworm.exceptions.HttpConnectionError`). Both share the
`meta["retry_times"]` budget and exponential backoff. Callback bugs, redirect
loops, TLS errors, and oversized responses are not retried. Pass
`retry_exceptions=()` to retry statuses only.

Register it in `response_middlewares`; the engine also calls its
`process_exception` hook for failed requests.

Requests time out after 60 seconds by default (`request_timeout`; `None`
disables it, and `Request.timeout` overrides it per request), so a server that
stops responding cannot hold a worker indefinitely. Timeouts raise
`HttpTimeoutError` and are retried like other transient failures.

The budget covers sending the request and downloading the entire body, so raise
it for large or slow downloads (for example `Request(url, timeout=300)` for one
file). Each redirect hop gets a fresh budget, and time spent waiting for a free
concurrency slot does not count.

## Stop limits

These end the crawl gracefully (pending requests are discarded, or kept when a
[job directory](#pausing-and-resuming) is set; in-flight requests finish;
pipelines close normally) and report the limit as `close_reason`:

| Option | Stops when |
| --- | --- |
| `max_requests` | this many requests were sent |
| `max_items` | this many items passed every pipeline (exact; later items are dropped with reason `max_items`) |
| `max_errors` | this many unrecovered failures occurred |
| `max_duration` | this much time (seconds or `timedelta`) elapsed |

`max_depth` does not stop the crawl; it drops requests more than that many links
away from a start request. Each request's depth is recorded in
`request.meta["depth"]` (start requests have depth `0`).

To stop from your own code, raise {class}`~silkworm.exceptions.CloseSpider`
from a callback, middleware, or pipeline, or call `engine.stop(reason)`:

```python
from silkworm import CloseSpider

async def parse(self, response):
    if await response.select_first(".captcha"):
        raise CloseSpider("blocked_by_captcha")
```

## Graceful shutdown

The synchronous runners (and the CLI) handle SIGINT (Ctrl+C) and SIGTERM
(systemd, Kubernetes): the first signal stops the crawl gracefully, finishing
in-flight requests and closing pipelines so buffered items are flushed; a second
signal cancels immediately (pipelines are still closed). The crawl result then
has `close_reason == "shutdown"`.

`crawl()` does not install handlers by default because the calling application
owns the event loop; pass `crawl(spider, handle_signals=True)` to opt in, or call
`engine.stop()` from your own handler.

## Staying on-site

Set `allowed_domains` on the spider to drop requests to other hosts (subdomains
are allowed). Start requests are filtered too; requests with `dont_filter=True`
bypass the filter. Dropped requests are counted as `offsite_filtered`.

```python
class DocsSpider(Spider):
    start_urls = ("https://docs.example.com/",)
    allowed_domains = ("example.com",)  # also www.example.com, docs.example.com
```

Redirects are followed inside the HTTP client, so a redirect may still land on
another host; check `response.url` in callbacks if that matters.

## Politeness

### Per-domain concurrency

`concurrency` limits all fetches; `concurrency_per_domain` additionally limits
simultaneous fetches to the same host, so a broad crawl does not hammer one site.
A worker waits for its host's slot, so keep `concurrency` well above
`concurrency_per_domain` when crawling many hosts.

### AutoThrottle

{class}`~silkworm.middlewares.AutoThrottleMiddleware` spaces requests per host and
adapts the spacing to the host's latency: after each response the delay moves
toward `latency / target_concurrency`, throttling statuses (429/503) double it
and honour `Retry-After`, and error responses never make it faster. Register the
same instance as a request and a response middleware.

`DelayMiddleware` in contrast pauses every request by a fixed or random amount,
regardless of host.

### robots.txt

{class}`~silkworm.middlewares.RobotsTxtMiddleware` fetches `robots.txt` once per
origin and drops disallowed requests (counted as `ignored_requests` with reason
`robots_txt`). It also applies `Crawl-delay` (including decimal values) unless
`obey_crawl_delay=False`. A 4xx robots.txt means "no restrictions"; unreachable
or failing robots.txt files are allowed by default or skipped entirely with
`on_unavailable="disallow"`. Set `meta["dont_obey_robotstxt"] = True` to exempt
one request.

## Deduplication

The default deduplication key is {func}`~silkworm.request_fingerprint`: the
method, the canonical URL (see {func}`~silkworm.canonicalize_url`: lowercase
scheme and host, no default port or fragment, normalized percent-encoding,
sorted query parameters), `Request.params`, and the body. So `/a?x=1&y=2`,
`/A?y=2&x=1#top`, and `Request("/a", params={"x": 1, "y": 2})` are one page,
while a `POST` with a different JSON body is another. Pass `dedup_key=` for a
custom rule and `dont_filter=True` to bypass deduplication.

Keys are stored as 16-byte digests, which keeps the seen-set small. For crawls
with many millions of URLs, use a job directory to keep it on disk instead.

## Memory and response size

- `max_response_size_bytes` (default 50 MB) caps downloaded bodies. A larger
  `Content-Length` fails before the body is read, and streamed bodies stop as
  soon as they exceed the cap, raising
  {class}`~silkworm.exceptions.ResponseTooLargeError`. Override per request with
  `meta["max_response_size"]` (bytes, or `None` for no limit).
- `html_max_size_bytes` separately limits how much of a document is parsed.
- `max_pending_requests` bounds the queue (see
  [Queue Capacity](engine-and-http.md#queue-capacity-and-deadlock-freedom)).
- Labeled statistics keep at most 1000 labels each (for example domains); the
  long tail is summed under `_other`.

## Pausing and resuming

Pass `job_dir` to persist crawl state in a SQLite database:

```python
run_spider(MySpider, job_dir="state/my-spider")
```

Every request is written to the job when it is scheduled and deleted only after
its callback (and everything that callback scheduled) finished. If the crawl is
stopped (a signal, `stop()`, or a stop limit) or crashes, the next run with the
same `job_dir` restores the unfinished requests and skips everything already
seen, then runs `start_requests()` again (already-seen start requests are
skipped). Delivery is at-least-once: a request that was in flight during a crash
is fetched again, so pipelines should tolerate occasional duplicate items. When
a crawl finishes normally, the job is marked finished and the next run starts
fresh.

Stop limits combine well with jobs to crawl in batches:
`run_spider(MySpider, job_dir=..., max_requests=10_000)` continues where the
previous batch stopped.

Requirements, checked when a request is scheduled:

- Callbacks and errbacks must be methods of the spider (restored by name).
- `meta`, `params`, and bodies must be JSON-serializable (bytes bodies are fine).
- A job directory belongs to one spider name.

The seen-set lives in the database too, so memory no longer grows with the
number of URLs crawled.

## HTTP cache for development

While writing selectors, cache responses on disk so re-runs are instant and do
not hit the site again:

```python
from silkworm import HttpCache, run_spider

run_spider(MySpider, http_cache=HttpCache(".silkworm/httpcache"))
```

Entries are keyed by request fingerprint and marked with an
`x-silkworm-cache: hit` response header when served from disk. Options:
`expiration` (maximum age), `ignore_statuses` (server errors and 429 are never
stored by default), and `methods` (`GET`/`HEAD` by default). Set
`meta["dont_cache"] = True` to bypass the cache for one request. The cache wraps
any client, including browser-rendering ones.

## Observability

Periodic (`log_stats_interval`) and final statistics logs include every counter
and the labeled breakdowns. For dashboards and alerts, serve Prometheus metrics
while crawling:

```python
run_spider(MySpider, metrics_port=9410)  # http://127.0.0.1:9410/metrics
```

Counters are exported as `silkworm_<counter>_total{spider="..."}` (labeled ones
with a `status`, `domain`, `type`, or `reason` label), gauges as
`silkworm_queue_size`, `silkworm_in_flight`, `silkworm_seen_requests`,
`silkworm_memory_mb`, `silkworm_elapsed_seconds`, and `silkworm_running`, and
numeric spider `stats_payload` values as `silkworm_custom_<name>`. Use
`metrics_host="0.0.0.0"` to expose the endpoint beyond localhost, and
`engine.metrics_text()` to render the same text yourself.

## Settings

Runners and the CLI resolve engine options from four layers (lowest first):
engine defaults, `SILKWORM_<OPTION>` environment variables, the spider's
`custom_settings`, and options passed explicitly.

```python
class NewsSpider(Spider):
    custom_settings = {"concurrency": 8, "max_depth": 3, "request_timeout": 20}
```

```bash
SILKWORM_MAX_ITEMS=500 SILKWORM_JOB_DIR=state/news silkworm crawl news.py -o news.jl
```

Scalar options (numbers, booleans, strings, and paths such as `job_dir` and
`http_cache`) can be configured this way; middlewares, pipelines, and clients are
passed in code. Values are validated, so `SILKWORM_MAX_DEPTH=three` fails with an
error naming the variable. `custom_settings` keys that are not engine options are
left for your own use. `Engine(...)` itself takes its arguments literally; see
{mod}`silkworm.settings` to resolve the layers yourself.

## Validating items

{class}`~silkworm.pipelines.ValidationPipeline` checks items against a Pydantic
model (anything with `model_validate`) or a validator function. Valid items
continue normalized (`model_dump(mode="json")`); invalid ones are dropped with
reason `invalid` and the first few failures are logged with their errors.
Combined with `max_item_drop_rate` or `min_items`, a site redesign that breaks
extraction fails the crawl instead of silently producing empty data.

```python
from pydantic import BaseModel
from silkworm.pipelines import ValidationPipeline


class Product(BaseModel):
    title: str
    price: float


run_spider(
    ProductsSpider,
    item_pipelines=[ValidationPipeline(Product), JsonLinesPipeline("products.jl")],
    max_item_drop_rate=0.05,
)
```

Any pipeline can discard an item by raising
{class}`~silkworm.exceptions.DropItem`; later pipelines are skipped and `emit()`
returns normally.

## Testing spiders

{mod}`silkworm.testing` runs callbacks exactly as the engine does (same
`emit`/`follow` scope and callback checks) without network access, so spiders
can be tested against saved pages:

```python
from silkworm.testing import response_from_file, run_callback


async def test_parse_extracts_products():
    spider = ProductsSpider()
    response = response_from_file(
        "tests/fixtures/products.html",
        url="https://shop.example.com/products",
        callback=spider.parse,
    )

    result = await run_callback(spider.parse, response)

    assert result.items[0] == {"title": "Keyboard", "price": 99.5}
    assert result.urls == ["https://shop.example.com/products?page=2"]
```

Helpers: `html_response(body, url)`, `response_from_file(path, url)`,
`run_callback(callback, response)`, `run_errback(errback, request, exc)`, and
`run_start_requests(spider)`. Callback exceptions propagate unchanged, so tests
show the real traceback. For a quick manual check against the live site, use
`silkworm parse <url> --spider <file>` (see [CLI](cli.md)).

## Control-flow exceptions

| Exception | Raise from | Effect |
| --- | --- | --- |
| `IgnoreRequest(message, reason=...)` | request/response middleware | Drop the request; counted as `ignored_requests`; no errback. |
| `DropItem(message, reason=...)` | item pipeline | Discard the item; counted as `items_dropped`. |
| `CloseSpider(reason)` | callback, middleware, pipeline | Stop the crawl gracefully with `reason`. |
