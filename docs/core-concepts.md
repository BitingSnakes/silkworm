# Core Concepts

This section covers Silkworm's **Spider/Request/Response** model, callback semantics, and how data flows through the engine.

## Spider
**Spider** is the base class you subclass for each crawl. See [src/silkworm/spiders.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/spiders.py).

Key attributes and hooks:
- **`name`**: Spider identifier used in logs and stats.
- **`start_urls`**: Seed URLs for `start_requests()`.
- **`custom_settings`**: Per-spider settings storage (copied on init).
- **`start_requests()`**: Async generator that yields initial `Request` objects.
- **`parse(response)`**: Main callback (auto-wrapped to `HTMLResponse`).
- **`open()` / `close()`**: Lifecycle hooks called by the engine.

```python
from silkworm import Response, Spider


class MySpider(Spider):
    name = "my_spider"
    start_urls = ("https://example.com",)

    async def parse(self, response: Response):
        yield {"url": response.url, "status": response.status}
```

## Request
`Request` is a slotted dataclass used to describe HTTP work. See [src/silkworm/request.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/request.py).

Important fields:
- **`url`**, **`method`**, **`headers`**, **`params`**, **`data`**, **`json`**
- **`timeout`**: Per-request timeout (seconds or `timedelta`).
- **`callback`**: Callback to run with the response.
- **`errback`**: Callback to run when the request fails and no middleware retries it.
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
failed `Request` and the raised exception, and it can return or yield the same shapes
as a normal callback: items, follow-up requests, iterables, async iterables, or `None`.

```python
from silkworm import Request

async def start_requests(self):
    yield Request(
        url="https://example.com/maybe-down",
        callback=self.parse,
        errback=self.handle_error,
    )

async def handle_error(self, request: Request, exception: Exception):
    yield {
        "url": request.url,
        "error_type": exception.__class__.__name__,
    }
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
- **`follow(href, callback=None, **kwargs)`**: URL join + callback reuse.
- **`follow_all(hrefs, callback=None, **kwargs)`**: Convenience helper for multiple follow-up requests.
- **`close()`**: Release payload references to save memory.
- **`await to_markdown(mode="full" | "minimal" | "mdream", options=None)`**: Convert an `HTMLResponse` to Markdown via `fast-h2m` (runs off the event-loop thread).
- **`await to_markdown_result(...)`**: Return `fast-h2m`'s structured conversion result.

```python
from silkworm import HTMLResponse, Response

async def parse(self, response: Response):
    if not isinstance(response, HTMLResponse):
        return

    title = await response.select_first("title")
    if title:
        yield {"title": title.text.strip()}
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

## Callback Results (What `parse` Can Return)
Callback output is normalized by the engine. See [src/silkworm/engine.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/engine.py).

Valid outputs:
- A single **item** (JSON-like object)
- A **Request**
- An **iterable** of items and/or requests
- An **async iterable** of items and/or requests
- An **awaitable** that resolves to any of the above
- **`None`**

Example of mixed results:

```python
from silkworm import Request

async def parse(self, response: Response):
    return [
        {"url": response.url},
        Request(url="https://example.com/page2", callback=self.parse),
        {"ok": True},
        {"ok": False},
    ]
```

> **Note:** The engine auto-wraps **only** the spider's `parse` callback to `HTMLResponse`. Other callbacks receive the `Response` produced by the HTTP client, which may already be an `HTMLResponse` for HTML content.

## Errors
All framework exceptions derive from `SilkwormError` and are importable from `silkworm`. See [src/silkworm/exceptions.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/exceptions.py).

| Exception | Raised when |
| --- | --- |
| `HttpError` | A fetch fails: network errors, timeouts, redirect loops, a closed client, or CDP/Servo/OnionLink failures. |
| `SpiderError` | A spider callback raises, or yields a value the engine cannot handle. The original exception is chained as `__cause__`. |
| `SelectorError` | CSS/XPath selector evaluation or HTML parsing fails. |
| `MarkdownConversionError` | HTML-to-Markdown conversion fails. |
| `DeclarativeError` (and subclasses) | Declarative item extraction fails; see [Declarative Extraction](declarative.md#errors). |

Recover from failed requests with `Request.errback` or an exception middleware (see [Middlewares](middlewares.md)).

## Deduplication
The engine keeps a set of seen request keys. The default key is `Request.url`; pass `dedup_key` to `Engine`, `crawl`, or `run_spider` if params, method, or body should be part of the key.

```python
from silkworm import Request

yield Request(url=same_url, dont_filter=True)
```

Or customize the key globally for a run:

```python
from urllib.parse import urlencode

from silkworm import Request, run_spider


def dedup_with_params(req: Request) -> str:
    return f"{req.url}?{urlencode(req.params, doseq=True)}"


run_spider(MySpider, dedup_key=dedup_with_params)
```

## Data Types
Public type aliases and protocols live in [`silkworm.types`](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/types.py). Import them from there to annotate your own code, for example:

```python
from silkworm.types import Callback, JSONValue, Logger, MetaData
```

It covers:

- **JSON item shapes**: `JSONScalar`, `JSONValue`, and the read-only `JSONLike` that callbacks may yield.
- **Request data**: `Headers`, `QueryParams`, `QueryValue`, `MetaData`, `BodyData`.
- **Callbacks**: `Callback`, `CallbackOutput`, `CallbackResult`, `Errback`.
- **Engine and runners**: `EngineOptions`, `DedupKey`, `LoopFactory`.
- **Logging**: the `Logger` protocol and `LogLevel`.
- **Middleware and pipeline protocols**: `RequestMiddleware`, `ResponseMiddleware`, `ExceptionMiddleware`, `ItemPipeline`, plus `ItemCallback` (for `CallbackPipeline`) and `ZenohKeyResolver` (for `ZenohPipeline`).

See the [`silkworm.types` reference](api/types.md) for each definition.
