# Core Concepts

This section covers Silkworm's **Spider/Request/Response** model, callback semantics, and how data flows through the engine.

## Spider
**Spider** is the base class you subclass for each crawl. See [src/silkworm/spiders.py](../src/silkworm/spiders.py).

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
`Request` is an immutable dataclass used to describe HTTP work. See [src/silkworm/request.py](../src/silkworm/request.py).

Important fields:
- **`url`**, **`method`**, **`headers`**, **`params`**, **`data`**, **`json`**
- **`timeout`**: Per-request timeout (seconds or `timedelta`).
- **`callback`**: Callback to run with the response.
- **`meta`**: Free-form dict for middlewares and custom logic.
- **`dont_filter`**: Bypass URL deduplication.
- **`priority`**: Reserved for future queueing (not used by the engine yet).

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

`Request.replace(**kwargs)` returns a copy with updates:

```python
updated = request.replace(dont_filter=True, headers={"x-trace": "1"})
```

### Built-in `meta` Keys
These are used by built-in components (you can add your own as well):
- **`proxy`**: Used by `ProxyMiddleware` and [HttpClient](../src/silkworm/http.py) for proxy routing.
- **`retry_times`**: Used by `RetryMiddleware` to track attempts.
- **`allow_non_html`**: Used by `SkipNonHTMLMiddleware` to bypass filtering.
- **`redirect_times`**: Set by [HttpClient](../src/silkworm/http.py) when following redirects.

## Response and HTMLResponse
`Response` contains the response payload; `HTMLResponse` adds selector helpers. See [src/silkworm/response.py](../src/silkworm/response.py).

Core APIs:
- **`text`**: Decoded body text with charset detection.
- **`encoding`**: Detected or default encoding.
- **`url_join(href)`**: Resolve a relative URL against the response URL.
- **`follow(href, callback=None, **kwargs)`**: URL join + callback reuse.
- **`follow_all(hrefs, callback=None, **kwargs)`**: Convenience helper for multiple follow-up requests.
- **`close()`**: Release payload references to save memory.

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

Elements returned from these helpers also expose async selectors, so nested lookups should be awaited:

```python
for card in await response.select(".card"):
    title = await card.select_first("h2")
```

The selector engine uses `scraper-rs` and respects `doc_max_size_bytes` (see [HttpClient](../src/silkworm/http.py)). Errors are raised as `SelectorError` in [src/silkworm/exceptions.py](../src/silkworm/exceptions.py).

## Callback Results (What `parse` Can Return)
Callback output is normalized by the engine. See [src/silkworm/engine.py](../src/silkworm/engine.py).

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
    yield {"url": response.url}
    yield Request(url="https://example.com/page2", callback=self.parse)
    return [{"ok": True}, {"ok": False}]
```

> **Note:** The engine auto-wraps **only** the spider's `parse` callback to `HTMLResponse`. Other callbacks receive the `Response` produced by the HTTP client, which may already be an `HTMLResponse` for HTML content.

## Deduplication
The engine keeps a set of seen URLs. If a request has the same URL and `dont_filter` is **False**, it is skipped. See [src/silkworm/engine.py](../src/silkworm/engine.py).

```python
from silkworm import Request

yield Request(url=same_url, dont_filter=True)
```

## Data Types
Shared JSON-friendly types live in [src/silkworm/_types.py](../src/silkworm/_types.py). Use these as guides for item structures and metadata.
