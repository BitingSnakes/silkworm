# Middlewares

Middlewares let you intercept requests and responses. The engine applies them in order. See [src/silkworm/middlewares.py](../src/silkworm/middlewares.py).

## Interfaces
Middlewares implement protocol-style async methods:

```python
class RequestMiddleware:
    async def process_request(self, request, spider) -> Request: ...

class ResponseMiddleware:
    async def process_response(self, response, spider) -> Response | Request: ...

class ExceptionMiddleware:
    async def process_exception(self, request, exception, spider) -> Request | None: ...
```

Order of execution:
1. **Request middlewares** (before HTTP fetch)
2. **Response middlewares** (after HTTP fetch)
3. **Callback** (`parse` or custom callback)

If request processing fails, the engine calls `process_exception` on middleware
instances from both middleware lists, deduplicating shared instances. Returning a
`Request` schedules a retry; returning `None` leaves the exception for the next
exception middleware or the request's `errback`.

```python
run_spider(
    MySpider,
    request_middlewares=[UserAgentMiddleware(), DelayMiddleware(delay=0.5)],
    response_middlewares=[RetryMiddleware(max_times=3), SkipNonHTMLMiddleware()],
)
```

## Built-in Middlewares

### UserAgentMiddleware
- Picks a random user agent from a list or uses the default `silkworm/0.1`.
- Code: [src/silkworm/middlewares.py](../src/silkworm/middlewares.py)

```python
UserAgentMiddleware(user_agents=["UA1", "UA2"], default="silkworm/0.1")
```

### ProxyMiddleware
- Rotates proxies (round-robin or random).
- Reads from a list or file.
- Writes `request.meta["proxy"]` for the HTTP client.
- Retries fetch exceptions with another proxy when one is available, preserving failed proxies in request metadata.
- Code: [src/silkworm/middlewares.py](../src/silkworm/middlewares.py)

```python
ProxyMiddleware(proxies=["http://proxy1:8080", "http://proxy2:8080"])
ProxyMiddleware(proxy_file="proxies.txt", random_selection=True)
```

### CookiesMiddleware
- Stores `Set-Cookie` response headers and applies matching `Cookie` headers to later requests.
- Uses Python's standard `CookieJar`, so domain, path, secure, expiry, and session-cookie rules are handled by the jar.
- Use the same middleware instance in `request_middlewares` and `response_middlewares`.
- Saves and loads Netscape/Mozilla cookie files for reuse across spider runs.
- Code: [src/silkworm/middlewares.py](../src/silkworm/middlewares.py)
- Example: [examples/cookie_reuse_spiders.py](../examples/cookie_reuse_spiders.py)

```python
from pathlib import Path

from silkworm import CookiesMiddleware, run_spider

cookies = CookiesMiddleware()

run_spider(
    LoginSpider,
    request_middlewares=[cookies],
    response_middlewares=[cookies],
)
cookies.save("data/cookies.txt")

reuse_cookies = CookiesMiddleware()
reuse_cookies.load("data/cookies.txt")

run_spider(
    AuthenticatedSpider,
    request_middlewares=[reuse_cookies],
    response_middlewares=[reuse_cookies],
)
```

Per-request controls:

```python
from silkworm import Request

# Add cookies only for this request, then let the jar manage them.
yield Request(
    url="https://example.com/account",
    meta={"cookies": {"session": "abc"}},
)

# Use isolated cookie sessions within one crawl.
yield Request(
    url="https://example.com/account",
    meta={"cookiejar": "account-a"},
)

# Bypass cookie storage and cookie header merging for this exchange.
yield Request(
    url="https://example.com/public",
    meta={"dont_merge_cookies": True},
)
```

Useful constructor and helper options:

- `CookiesMiddleware(cookies={"name": "value"})`: seed the default jar with cookies.
- `CookiesMiddleware(allow_domains=["example.com"])`: accept/send cookies only for allowed domains.
- `CookiesMiddleware(block_domains=["tracking.example"])`: reject blocked domains.
- `CookiesMiddleware(hide_cookie_header=False)`: preserve a manually supplied `Cookie` header instead of replacing it with the jar-managed header.
- `cookies.set_cookie("sid", "abc", domain="example.com")`: add one cookie programmatically.
- `cookies.clear()`: clear all jars.
- `cookies.clear("account-a")`: clear a named jar.
- `cookies.clear_session_cookies()`: discard session cookies while keeping persistent cookies.

### DelayMiddleware
- Fixed, random range, or custom delay function.
- Uses `asyncio.sleep` (non-blocking).
- Code: [src/silkworm/middlewares.py](../src/silkworm/middlewares.py)

```python
DelayMiddleware(delay=1.0)
DelayMiddleware(min_delay=0.3, max_delay=1.0)

def custom_delay(request, spider) -> float:
    return 0.5

DelayMiddleware(delay_func=custom_delay)
```

### RetryMiddleware
- Retries on HTTP codes (defaults include 500, 502, 503, 504, 522, 524, 408, 429).
- Exponential backoff via `backoff_base`.
- Uses `request.meta["retry_times"]` and sets `dont_filter=True` on retries.
- Code: [src/silkworm/middlewares.py](../src/silkworm/middlewares.py)

```python
RetryMiddleware(max_times=3, backoff_base=0.5, sleep_http_codes=[429, 503])
```

### SkipNonHTMLMiddleware
- Skips callbacks for non-HTML responses unless `allow_non_html` is set in request meta.
- Checks content-type and optional body sniff.
- Code: [src/silkworm/middlewares.py](../src/silkworm/middlewares.py)

```python
SkipNonHTMLMiddleware(allowed_types=["html"], sniff_bytes=2048)
```

### RequestResponseStreamMiddleware
- Streams paired `request` and `response` telemetry events to a collector endpoint.
- Use the same instance in `request_middlewares` and `response_middlewares`.
- Adds internal exchange IDs to request metadata so downstream systems can join events.
- Supports authorization headers, bounded sender queue, body truncation, batching, and `open`/`close` lifecycle flushing.
- Code: [src/silkworm/middlewares.py](../src/silkworm/middlewares.py)
- Example: [examples/request_response_stream_spider.py](../examples/request_response_stream_spider.py)

```python
from silkworm import RequestResponseStreamMiddleware

stream = RequestResponseStreamMiddleware(
    "https://collector.example.com/events",
    auth_token="secret-token",
    batch_size=50,
    max_body_bytes=8_192,
)

run_spider(
    MySpider,
    request_middlewares=[stream],
    response_middlewares=[stream],
)
```

### CloudflareCrawlMiddleware
- Routes opt-in requests through Cloudflare Browser Rendering's crawl API.
- Enable per request with `request.meta["cloudflare_crawl"] = True` or pass a dict of per-request crawl options.
- The callback receives a synthetic JSON `Response` containing the final Cloudflare API payload.
- Requires Cloudflare account credentials; there is no package extra for this middleware.
- Code: [src/silkworm/middlewares.py](../src/silkworm/middlewares.py)
- Example: [examples/cloudflare_crawl_spider.py](../examples/cloudflare_crawl_spider.py)

```python
from silkworm import Request
from silkworm.middlewares import CloudflareCrawlMiddleware

yield Request(
    url="https://example.com/",
    callback=self.parse,
    meta={"cloudflare_crawl": {"limit": 25, "render": True}},
)

run_spider(
    MySpider,
    request_middlewares=[
        CloudflareCrawlMiddleware(
            account_id="...",
            api_token="...",
            timeout=300,
        )
    ],
)
```

## Custom Middleware Example

```python
from silkworm.request import Request

class AddHeaderMiddleware:
    async def process_request(self, request: Request, spider):
        headers = {**request.headers}
        headers.setdefault("x-trace", "1")
        return request.replace(headers=headers)
```

```python
from silkworm.request import Request

class RetryOnceMiddleware:
    async def process_request(self, request: Request, spider):
        return request

    async def process_exception(
        self,
        request: Request,
        exception: Exception,
        spider,
    ) -> Request | None:
        if request.meta.get("retried"):
            return None
        return request.replace(meta={**request.meta, "retried": True}, dont_filter=True)
```
