# Migration Guide

This page collects behavior changes that may require updates when upgrading
Silkworm. Read every section between your installed version and the target
version.

## Upgrading to 0.13

- Every built-in pipeline now provides `process_items(items, spider)` for
  explicit batch processing. The default preserves the order, transformations,
  and errors of repeated `process_item` calls.
- `IggyPipeline` uses Apache Iggy's native producer batch operation. Install it
  with `pip install "silkworm-rs[iggy]"` on Python 3.13.
- `BatchItemPipeline` is available as a public protocol for batch-capable custom
  pipelines. Existing custom pipelines implementing only `ItemPipeline` remain
  valid.

The crawl engine remains streaming: every `await spider.emit(item)` still sends
one item through the configured pipeline chain. `process_items()` is an explicit
pipeline API for callers that already hold a batch.

## Upgrading to 0.12

- Runners and `Engine.run()` return a `CrawlResult` instead of `None`.
- The default deduplication key is the request fingerprint—method, canonical URL
  with `params`, and body—instead of the raw URL. Equivalent query ordering is
  deduplicated, while POST requests with different bodies remain distinct.
- `items_scraped` counts items that passed every pipeline. Pipelines can raise
  `DropItem` to discard an item, which increments `items_dropped`.
- Responses preserve the exact downloaded bytes. Bodies larger than
  `max_response_size_bytes`—50 MB by default—raise
  `ResponseTooLargeError`.
- Timeouts and connection failures raise `HttpTimeoutError` and
  `HttpConnectionError`, both subclasses of `HttpError`. `RetryMiddleware`
  retries them.
- Requests store link depth in `request.meta["depth"]`. Built-in counter names,
  such as `retries`, are reserved in `Spider.stats_payload`.
- Synchronous runners stop gracefully on SIGINT and SIGTERM. Pass
  `handle_signals=False` to opt out.
- Requests time out after 60 seconds by default. Pass `request_timeout=None` to
  restore unlimited waits.

See [Production Crawling](production.md) for failure policies and stop behavior,
and [Engine and HTTP Client](engine-and-http.md) for request semantics.

## Upgrading from 0.10 to 0.11

Silkworm 0.11 replaced yielded and returned callback outputs with push-style
`emit` and `follow` calls. Callbacks, errbacks, and `start_requests()` are async
functions returning `None`.

| Before 0.11 | 0.11 and newer |
| --- | --- |
| `yield item` | `await self.emit(item)` |
| `yield request` | `await self.follow(request)` |
| `yield response.follow(href)` | `await response.follow(href)` |
| return a list of outputs | await each `emit` or `follow` call |

Legacy generator callbacks fail with a `SpiderError` containing a migration
hint. See [Reporting Results](core-concepts.md#reporting-results-emit-and-follow)
for callback lifetime and backpressure rules.
