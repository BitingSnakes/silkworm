# Middlewares

Middlewares let you intercept requests and responses. The engine applies them in order. See [src/silkworm/middlewares.py](../src/silkworm/middlewares.py).

## Interfaces
Middlewares implement protocol-style async methods:

```python
class RequestMiddleware:
    async def process_request(self, request, spider) -> Request: ...

class ResponseMiddleware:
    async def process_response(self, response, spider) -> Response | Request: ...
```

Order of execution:
1. **Request middlewares** (before HTTP fetch)
2. **Response middlewares** (after HTTP fetch)
3. **Callback** (`parse` or custom callback)

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
- Code: [src/silkworm/middlewares.py](../src/silkworm/middlewares.py)

```python
ProxyMiddleware(proxies=["http://proxy1:8080", "http://proxy2:8080"])
ProxyMiddleware(proxy_file="proxies.txt", random_selection=True)
```

### DelayMiddleware
- Fixed, random range, or custom delay function.
- Uses `asyncio.sleep` (non-blocking).
- Code: [src/silkworm/middlewares.py](../src/silkworm/middlewares.py)

```python
DelayMiddleware(delay=1.0)
DelayMiddleware(min_delay=0.3, max_delay=1.0)

async def custom_delay(request, spider) -> float:
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

## Custom Middleware Example

```python
from silkworm.request import Request

class AddHeaderMiddleware:
    async def process_request(self, request: Request, spider):
        request.headers.setdefault("x-trace", "1")
        return request
```
