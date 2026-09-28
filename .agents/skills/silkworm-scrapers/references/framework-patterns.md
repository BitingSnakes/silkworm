# Silkworm framework patterns

Use these patterns with the installed or repository version of Silkworm. Confirm signatures from local source when they differ.

## Required callback model

- Import `from __future__ import annotations` first.
- Subclass `Spider`; set `name`, `start_urls`, and usually `allowed_domains`.
- Type callbacks as `async def ...(...) -> None`.
- `parse` is the only callback the engine guarantees to wrap as `HTMLResponse`. Other callbacks may receive a plain `Response`, so narrow before selecting.
- Selectors on `HTMLResponse` and returned elements are async: `await response.select(...)`, `await element.select_first(...)`, and the CSS/XPath variants.
- Emit JSON-compatible items with `await self.emit(item)`.
- Schedule with `await self.follow(...)`, `await response.follow(...)`, or their `follow_all` variants. These calls apply backpressure.
- Do not use `yield`, async generators, or non-`None` callback returns; the engine rejects them with `SpiderError`.
- Tasks may emit or follow only while their callback is active. If concurrency within a callback is worthwhile, await every task before returning, preferably with `asyncio.TaskGroup`.

## Minimal imperative spider

```python
from __future__ import annotations

from silkworm import HTMLResponse, Response, Spider, run_spider
from silkworm.pipelines import JsonLinesPipeline


class ProductsSpider(Spider):
    name = "products"
    start_urls = ("https://example.com/products/",)
    allowed_domains = ("example.com",)

    async def parse(self, response: Response) -> None:
        if not isinstance(response, HTMLResponse):
            return

        for card in await response.select(".product"):
            title = await card.select_first("h2")
            link = await card.select_first("a")
            if title is None or link is None or not (href := link.attr("href")):
                continue
            await self.emit(
                {
                    "title": title.text.strip(),
                    "url": response.url_join(href),
                }
            )

        if next_link := await response.select_first("a.next"):
            if href := next_link.attr("href"):
                await response.follow(href, callback=self.parse)


if __name__ == "__main__":
    run_spider(
        ProductsSpider,
        item_pipelines=[JsonLinesPipeline("data/products.jl")],
        concurrency=4,
        request_timeout=20,
    )
```

Keep missing-field behavior intentional. Skip an incomplete record only if that matches the requested contract; otherwise emit a nullable field, validate and count a dropped item, or fail visibly.

## Custom starts, requests, and APIs

Override `start_requests()` when seeds need headers, parameters, POST data, JSON, metadata, callbacks, or errbacks:

```python
async def start_requests(self) -> None:
    await self.follow(
        "https://api.example.com/items",
        params={"page": 1},
        headers={"accept": "application/json"},
        callback=self.parse_api,
        errback=self.handle_error,
    )
```

`Request` supports `url`, `method`, `headers`, `params`, `data`, `json`, `timeout`, `meta`, `callback`, `errback`, `dont_filter`, and `priority`. Higher priority values are dequeued first. Prefer `Request.replace(...)` when changing an existing request. Built-ins use some `meta` keys, including `proxy`, `retry_times`, `allow_non_html`, `cookiejar`, `cookies`, and `dont_merge_cookies`; use distinct names for site-specific state.

For JSON responses, parse `response.text` with `json.loads`, check container and field types, then emit only JSON-compatible values. Follow API pagination using the endpoint's cursor or next URL, not HTML assumptions.

## Declarative extraction

Use `silkworm.declarative` when every record follows a regular field plan:

```python
from silkworm.declarative import Attr, Item, Text


class Product(Item):
    __selector__ = ".product"

    title: str = Text("h2", strip=True)
    price: str | None = Text(".price", strip=True)
    tags: list[str] = Text(".tag", strip=True)
    url: str = Attr("a", "href", absolute=True)


async def parse(self, response: Response) -> None:
    if not isinstance(response, HTMLResponse):
        return
    async for product in Product.extract(response):
        await self.emit(product.to_dict())
```

Annotations control cardinality: `T` is required, `T | None` is optional, and `list[T]` collects all matches. `transform` is synchronous and `default` handles a missing scalar. Keep pagination and irregular page logic in callbacks.

## Choosing output

Prefer a streaming pipeline for unbounded crawls. Common core choices are `JsonLinesPipeline`, `CSVPipeline`, `XMLPipeline`, `SQLitePipeline`, and `WebhookPipeline`; `ValidationPipeline` validates item shape before later pipelines. Some optional pipelines buffer all items until close, so inspect the relevant pipeline implementation or documentation before selecting one for a large crawl.

Do not add an optional dependency merely because a pipeline exists. Match the user's requested destination and the project's installed extras.

## Rendering and special transports

First confirm that the desired data is absent from the initial HTML and from a discoverable JSON endpoint. If browser rendering is genuinely required, use the fetch client supported by the installed Silkworm version (for example Servo or a configured CDP integration) and document its extra dependency and runtime requirement. Do not disguise rendering as a selector issue.
