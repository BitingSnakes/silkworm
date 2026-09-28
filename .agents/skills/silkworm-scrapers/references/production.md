# Production crawl controls

Read this reference for unattended, resumable, scheduled, or high-volume crawls.

## Bound and identify the crawl

- Set `allowed_domains` unless cross-domain crawling is explicitly required.
- Choose conservative `concurrency` and `concurrency_per_domain`; increase them only from observed latency and server tolerance.
- Use `max_depth`, `max_requests`, `max_items`, or `max_duration` to make the intended boundary executable.
- Set a realistic `request_timeout` and retain the response-size limits unless the expected payload requires more.
- Use `job_dir` for pause/resume. Reusing a job directory also reuses crawl state, so isolate it per logical job.

## Middleware selection

- `RobotsTxtMiddleware`: enforce robots rules and crawl delay when appropriate.
- `AutoThrottleMiddleware`: adapt request rate to observed latency. Reuse the same instance in request and response middleware lists.
- `RetryMiddleware`: retry transient failures and selected statuses with a small, bounded attempt count.
- `UserAgentMiddleware`: set or rotate an honest user agent when needed.
- `CookiesMiddleware`: preserve sessions; use named `cookiejar` metadata when sessions must remain isolated.
- `SkipNonHTMLMiddleware`: drop unexpected non-HTML responses unless `meta["allow_non_html"]` is set.

Do not stack delay mechanisms without understanding their combined effect. Do not use proxies, cookie persistence, or rendering unless the target and deployment require them.

## Data quality and failure policy

Put validation before storage. `ValidationPipeline` can drop malformed items; use `min_items`, `max_error_rate`, and `max_item_drop_rate` so a layout change fails the job instead of producing an apparently successful empty file. Catch `CrawlFailedError` at the executable boundary, report `exc.result.failures`, and return a nonzero exit status.

Inspect the returned `CrawlResult`, especially `close_reason`, item counts, request counts, errors, elapsed time, and failure messages. Enable periodic stats or Prometheus metrics when the job must be monitored.

## Operational checks

- Ensure output behavior is safe on resume. Confirm whether the selected pipeline appends, overwrites, or buffers until close.
- Avoid logging credentials, authorization headers, cookies, or scraped personal data.
- Use environment variables or the deployment secret store for credentials.
- Smoke-test with a separate output and job directory before launching the full crawl.
- Verify graceful shutdown and one resume cycle for long-running jobs.

## Representative runner shape

```python
throttle = AutoThrottleMiddleware(start_delay=0.5, max_delay=20.0)
result = run_spider(
    MySpider,
    request_middlewares=[RobotsTxtMiddleware(), throttle],
    response_middlewares=[throttle, RetryMiddleware(max_times=3)],
    item_pipelines=[ValidationPipeline(ItemModel), JsonLinesPipeline(output_path)],
    concurrency=4,
    concurrency_per_domain=2,
    request_timeout=30,
    max_depth=5,
    max_duration=900,
    min_items=1,
    max_error_rate=0.1,
    max_item_drop_rate=0.1,
    job_dir=job_dir,
)
```

Tune values for the target and the requested success criteria; these numbers are illustrative, not universal defaults.
