# Core Concepts

This section covers Silkworm's **Spider/Request/Response** model, callback semantics, and how data flows through the engine.

## Spider
**Spider** is the base class you subclass for each crawl. See [src/silkworm/spiders.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/spiders.py).

Key attributes and hooks:
- **`name`**: Spider identifier used in logs and stats.
- **`start_urls`**: Seed URLs for `start_requests()`.
- **`allowed_domains`**: Domains (and subdomains) the crawl may visit; other hosts are dropped. See [Staying on-site](production.md#staying-on-site).
- **`custom_settings`**: Per-spider settings storage (copied on init).
- **`start_requests()`**: Coroutine that schedules initial requests with `await self.follow(...)`.
- **`parse(response)`**: Main callback (auto-wrapped to `HTMLResponse`).
- **`await emit(item)`**: Send a scraped item through the item pipelines.
- **`await follow(request_or_url, callback=None, **kwargs)`** / **`await follow_all(...)`**: Schedule requests for crawling.
- **`open()` / `close()`**: Lifecycle hooks called by the engine.

```python
from silkworm import Response, Spider


class MySpider(Spider):
    name = "my_spider"
    start_urls = ("https://example.com",)

    async def parse(self, response: Response) -> None:
        await self.emit({"url": response.url, "status": response.status})
```

## Request
`Request` is a slotted dataclass used to describe HTTP work. See [src/silkworm/request.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/request.py).

Important fields:
- **`url`**, **`method`**, **`headers`**, **`params`**, **`data`**, **`json`**
- **`timeout`**: Per-request timeout (seconds or `timedelta`).
- **`callback`**: Async callback to run with the response.
- **`errback`**: Async callback to run when the request fails and no middleware retries it.
- **`meta`**: Free-form dict for middlewares and custom logic.
- **`dont_filter`**: Bypass request deduplication.
- **`priority`**: Higher values are dequeued first; requests with the same priority keep FIFO order.

`Request.replace(**kwargs)` is the safest way to create updated requests, while
`headers` and `meta` are mutable dicts that middlewares may update in-place.

```python
from silkworm import Request

request = Request(
    url="https://example.com/search",
    method="GET",
    params={"q": "silkworm"},
    headers={"accept": "text/html"},
    timeout=5,
)
```

### Request Error Handling
Use `Request.errback` for per-request recovery from fetch, middleware, or callback
exceptions that were not handled by exception middlewares. The errback receives the
failed `Request` and the raised exception. Like a normal callback it is an `async`
function returning `None` that may `await self.emit(...)` items and
`await self.follow(...)` follow-up requests.

```python
from silkworm import Request

async def start_requests(self) -> None:
    await self.follow(
        "https://example.com/maybe-down",
        callback=self.parse,
        errback=self.handle_error,
    )

async def handle_error(self, request: Request, exception: Exception) -> None:
    await self.emit(
        {
            "url": request.url,
            "error_type": exception.__class__.__name__,
        }
    )
```

### Built-in `meta` Keys
These are used by built-in components (you can add your own as well):
- **`proxy`**: Used by `ProxyMiddleware` and [HttpClient](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/http.py) for proxy routing.
- **`retry_times`**: Used by `RetryMiddleware` to track attempts.
- **`allow_non_html`**: Used by `SkipNonHTMLMiddleware` to bypass filtering.
- **`cookiejar`**: Used by `CookiesMiddleware` to isolate named cookie sessions.
- **`cookies`**: Used by `CookiesMiddleware` to add cookies for one request.
- **`dont_merge_cookies`**: Used by `CookiesMiddleware` to bypass cookie storage and header merging for one exchange.
- **`redirect_times`**: Set by [HttpClient](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/http.py) when following redirects.
- **`cloudflare_crawl`**: Used by `CloudflareCrawlMiddleware`; set to `True` or a dict of crawl options.
- **`servo_javascript`**, **`servo_settle_ms`**, **`servo_user_agent`**, **`servo_screenshot`**, **`servo_full_page`**: Used by `ServoFetchClient`.
- **`onionlink_response_limit`**: Used by `OnionLinkClient` to override the per-response byte cap.

## Response and HTMLResponse
`Response` contains the response payload; `HTMLResponse` adds selector helpers. See [src/silkworm/response.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/response.py).

Core APIs:
- **`text`**: Decoded body text with charset detection.
- **`encoding`**: Detected or default encoding.
- **`url_join(href)`**: Resolve a relative URL against the response URL.
- **`await follow(href, callback=None, **kwargs)`**: URL join + callback reuse, then schedule the request.
- **`await follow_all(hrefs, callback=None, **kwargs)`**: Schedule every non-`None` link in order.
- **`close()`**: Release payload references to save memory.
- **`await to_markdown(mode="full" | "minimal" | "mdream", options=None)`**: Convert an `HTMLResponse` to Markdown via `fast-h2m` (runs off the event-loop thread).
- **`await to_markdown_result(...)`**: Return `fast-h2m`'s structured conversion result.

```python
from silkworm import HTMLResponse, Response

async def parse(self, response: Response) -> None:
    if not isinstance(response, HTMLResponse):
        return

    title = await response.select_first("title")
    if title:
        await self.emit({"title": title.text.strip()})
```

Selector helpers on `HTMLResponse` (async):
- **`select(selector)`**
- **`select_first(selector)`**
- **`css(selector)`**
- **`css_first(selector)`**
- **`xpath(xpath)`**
- **`xpath_first(xpath)`**
- **`find(selector)`**: Alias for `select_first`.
- **`prettify()`**: Return the parsed document formatted as readable HTML.

Elements returned from these helpers also expose async selectors, so nested lookups should be awaited:

```python
for card in await response.select(".card"):
    title = await card.select_first("h2")
```

The selector engine uses `scraper-rs` and respects `doc_max_size_bytes` (see [HttpClient](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/http.py)). Errors are raised as `SelectorError` in [src/silkworm/exceptions.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/exceptions.py).

## HTML to Markdown
Markdown conversion is available on `HTMLResponse` (above) and as standalone helpers from `silkworm`. See [src/silkworm/markdown.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/markdown.py).

- **`html_to_markdown(html, mode="full", options=None)`**: Convert a complete document to a Markdown string.
- **`convert_html_to_markdown(html, mode="full", options=None)`**: Return `fast-h2m`'s structured result (`MarkdownResult`, a dict).
- **`MarkdownStream(mode="minimal", options=None)`**: Incremental converter; call `process_chunk(html)` for each fragment and `finish()` at the end. Each call returns the Markdown available so far.
- **`stream_html_to_markdown(chunks, ...)`** / **`stream_html_to_markdown_async(chunks, ...)`**: Convert an iterable / async iterable of HTML chunks into one string.

`MarkdownMode` is `"full"` (rich converter, the default for whole documents), `"minimal"` (lean Fast DOM path, the default for streaming), or `"mdream"` (mdream-backed lean path). `options` (`MarkdownOptions`) is a mapping of extra `fast-h2m` options that override the mode defaults. Conversion failures raise `MarkdownConversionError`; an unknown mode raises `ValueError`.

```python
from silkworm import MarkdownStream, html_to_markdown

markdown = html_to_markdown("<h1>Title</h1><p>Hello</p>")

stream = MarkdownStream()
parts = [stream.process_chunk(chunk) for chunk in ("<h1>Ti", "tle</h1><p>Hi</p>")]
parts.append(stream.finish())
markdown = "".join(parts)
```

## Reporting Results: `emit` and `follow`
Callbacks are `async` functions that return `None`. Instead of yielding or
returning results, they push them to the engine while they run. See
[src/silkworm/spiders.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/spiders.py).

| Call | Effect |
| --- | --- |
| `await self.emit(item)` | Runs `item` through every item pipeline, in order. |
| `await self.follow(request)` | Schedules a ready `Request` (deduplicated, then queued). |
| `await self.follow(url, callback=None, **fields)` | Builds and schedules a `Request`. Inside a response callback the URL is resolved against the response and the callback is inherited, like `response.follow`. |
| `await self.follow_all(targets, ...)` | Calls `follow` for every non-`None` target, in order. |
| `await response.follow(href, ...)` / `await response.follow_all(hrefs, ...)` | Schedules links relative to that response. |

With the default `item_batch_size=1`, each `await emit` finishes after the item
has passed every pipeline. When batching is enabled, it finishes after bounded
enqueueing; the callback scope drains its pending batches before returning, so
pipeline errors still fail the callback. `follow`
waits while the queue is full if it is the only callback waiting; otherwise it
enqueues past the bound so workers keep crawling and cannot deadlock (see
[Queue Capacity](engine-and-http.md#queue-capacity-and-deadlock-freedom)).
Both modes apply backpressure. Per-item pipeline errors surface at the `emit`
call site; batch failures surface while the callback scope drains.

```python
from silkworm import Request, Response, Spider


class MixedSpider(Spider):
    name = "mixed"
    start_urls = ("https://example.com",)

    async def parse(self, response: Response) -> None:
        await self.emit({"url": response.url})
        await self.follow("/page2")  # relative to response.url, reuses parse
        await self.follow(
            Request(url="https://example.com/api", callback=self.parse_api)
        )

    async def parse_api(self, response: Response) -> None:
        await self.emit({"ok": response.status == 200})
```

### Concurrency inside a callback
`emit` and `follow` are bound to the running callback through a context
variable, so tasks spawned inside a callback can use them too. Wait for those
tasks before the callback returns; `asyncio.TaskGroup` does this for you:

```python
import asyncio


async def parse(self, response: HTMLResponse) -> None:
    async with asyncio.TaskGroup() as tg:
        for card in await response.select(".card"):
            tg.create_task(self.parse_card(card))


async def parse_card(self, card) -> None:
    title = await card.select_first("h2")
    if title is not None:
        await self.emit({"title": title.text})
```

### Rules the engine enforces
- `emit`/`follow` raise `SpiderError` when awaited outside `start_requests()`, a
  request callback, or an errback, and when a task calls them after its callback
  has returned. Calls already in progress when the callback returns are awaited
  before the request is marked done.
- A callback that is an async generator (uses `yield`), is not `async`, or
  returns a value other than `None` fails with a `SpiderError` explaining the fix.
- `emit` rejects `Request` objects (use `follow`), and `follow` accepts request
  fields only together with a URL target.

### Migrating from 0.10 (`yield`-based callbacks)

| 0.10 | 0.11 |
| --- | --- |
| `yield {"a": 1}` | `await self.emit({"a": 1})` |
| `yield Request(url, callback=cb)` | `await self.follow(url, callback=cb)` or `await self.follow(Request(...))` |
| `yield response.follow(href)` | `await response.follow(href)` |
| `for r in response.follow_all(hrefs): yield r` | `await response.follow_all(hrefs)` |
| `return [item, request]` | one `emit`/`follow` call per result |
| `async def start_requests(self): yield Request(...)` | `async def start_requests(self) -> None: await self.follow(...)` |
| `-> CallbackOutput` / `-> AsyncIterator[...]` | `-> None` |

> **Note:** The engine auto-wraps **only** the spider's `parse` callback to `HTMLResponse`. Other callbacks receive the `Response` produced by the HTTP client, which may already be an `HTMLResponse` for HTML content.

## Errors
All framework exceptions derive from `SilkwormError` and are importable from `silkworm`. See [src/silkworm/exceptions.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/exceptions.py).

| Exception | Raised when |
| --- | --- |
| `HttpError` | A fetch fails: redirect loops, a closed client, or CDP/Servo/OnionLink failures. Subclasses: `HttpTimeoutError` (timeouts), `HttpConnectionError` (refused, reset, or dropped connections), `ResponseTooLargeError` (body over `max_response_size_bytes`). |
| `SpiderError` | A spider callback (or `start_requests`/errback) raises, is not an `async` function returning `None`, or calls `emit`/`follow` outside its scope. The original exception is chained as `__cause__`. |
| `SelectorError` | CSS/XPath selector evaluation or HTML parsing fails. |
| `MarkdownConversionError` | HTML-to-Markdown conversion fails. |
| `DeclarativeError` (and subclasses) | Declarative item extraction fails; see [Declarative Extraction](declarative.md#errors). |
| `CrawlFailedError` | The crawl violated its failure policy; `exc.result` holds the `CrawlResult`. See [Production Crawling](production.md#failure-policy). |
| `BatchPipelineError` | A bulk destination reported one or more rejected items; count details are available on the exception. |

Three exceptions are control-flow signals rather than errors: `IgnoreRequest`
(drop a request from a middleware), `DropItem` (discard an item from a pipeline),
and `CloseSpider` (stop the crawl gracefully). See
[Control-flow exceptions](production.md#control-flow-exceptions).

Recover from failed requests with `Request.errback` or an exception middleware (see [Middlewares](middlewares.md)).

## Deduplication
The engine keeps a set of seen request keys. The default key is
`request_fingerprint(request)`: the HTTP method, the canonical URL (lowercase
scheme and host, no default port or fragment, sorted query parameters) with
`Request.params` merged in, and the request body. Headers and `meta` are ignored.
See [Deduplication](production.md#deduplication) for details, and pass
`dedup_key` to `Engine`, `crawl`, or `run_spider` for a custom rule.

```python
await self.follow(same_url, dont_filter=True)
```

Or customize the key globally for a run, for example to treat URLs as equal
regardless of their query string:

```python
from silkworm import Request, canonicalize_url, run_spider


def dedup_without_query(req: Request) -> str:
    return canonicalize_url(req.url).split("?", 1)[0]


run_spider(MySpider, dedup_key=dedup_without_query)
```

## Data Types
Public type aliases and protocols live in [`silkworm.types`](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/types.py). Import them from there to annotate your own code, for example:

```python
from silkworm.types import Callback, JSONValue, Logger, MetaData
```

It covers:

- **JSON item shapes**: `JSONScalar`, `JSONValue`, and the read-only `JSONLike` accepted by `Spider.emit`.
- **Request data**: `Headers`, `QueryParams`, `QueryValue`, `MetaData`, `BodyData`.
- **Callbacks**: `Callback` and `Errback` (async callables returning `None`).
- **Engine and runners**: `EngineOptions`, `DedupKey`, `LoopFactory`, `CrawlResult`, and the `FetchClient` protocol for custom HTTP clients.
- **Logging**: the `Logger` protocol and `LogLevel`.
- **Middleware and pipeline protocols**: `RequestMiddleware`, `ResponseMiddleware`, `ExceptionMiddleware`, `ItemPipeline`, `BatchItemPipeline`, plus `ItemCallback` (for `CallbackPipeline`), `ItemSchema`/`ItemValidator`/`ModelSchema` (for `ValidationPipeline`), and `ZenohKeyResolver` (for `ZenohPipeline`).

See the [`silkworm.types` reference](api/types.md) for each definition.
