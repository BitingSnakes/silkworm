---
name: silkworm-scrapers
description: Build, adapt, debug, or review async web spiders and crawlers with the Silkworm Python framework. Use for site-specific extraction, pagination, API-backed pages, output pipelines, and offline spider tests; do not use for generic scraping code in Scrapy, Beautiful Soup, Playwright, or other frameworks unless the user asks to migrate it to Silkworm.
---

# Silkworm Scrapers

Create a runnable scraper that follows the site's real structure, emits the requested schema, and is easy to verify when the page changes.

## Start with evidence

Before writing selectors, inspect the supplied HTML, fixtures, API responses, or target pages. Keep live discovery narrow: fetch only enough pages to identify record containers, required fields, pagination, and whether content is server-rendered. Respect the site's access rules and do not add CAPTCHA solving, authentication bypasses, or evasive anti-bot behavior.

If working inside the Silkworm repository, prefer the current source, tests, and examples over remembered APIs. In another project, inspect its pinned `silkworm-rs` version and existing spiders before choosing features.

Clarify only choices that materially change the result and cannot be inferred, such as the required item fields or output destination. Otherwise choose conservative defaults and state them briefly.

## Build the spider

Read [references/framework-patterns.md](references/framework-patterns.md) before writing or reviewing Silkworm code.

1. Define the emitted item shape from the request and observed page. Normalize text and URLs deliberately; do not silently invent missing values.
2. Choose ordinary async selectors for irregular pages. Choose declarative `Item` fields only when records have a stable, repeated structure.
3. Keep callbacks typed, asynchronous, and push-based. Every callback returns `None`; it reports work with `await self.emit(...)` and `await self.follow(...)` or `await response.follow(...)`. Never yield or return items or requests.
4. Guard HTML-only parsing with `isinstance(response, HTMLResponse)`. Await every Silkworm selector, including selectors on nested elements.
5. Resolve relative links through `response.follow`, `self.follow` inside a response callback, or `response.url_join`; avoid hand-built URL concatenation.
6. Give multi-step pages separate callbacks and pass small identifiers through `Request.meta`. Parse JSON endpoints from `Response.text` with explicit validation of the expected shape.
7. Keep concurrency bounded and crawling polite. Add middleware or rendering only when the target behavior justifies it.

Use [assets/spider.py](assets/spider.py) as a starting point when creating a new module, but replace its example domain, selectors, schema, and limits instead of leaving generic placeholders.

## Verify the result

For a new or changed parser, capture a small representative HTML/JSON fixture when permitted and test it with `silkworm.testing`. Assert exact items and followed URLs, including a missing optional field and the final page with no next link. Use [assets/test_spider.py](assets/test_spider.py) as an adaptable test skeleton.

Run focused tests first, then the project's formatter, linter, and type checker. In this repository, follow `AGENTS.md` and use the `just` or `uv` commands it defines.

Do a small live smoke run only when network access and the user's scope permit it. Cap pages or items during development and inspect output for duplicates, blank required fields, malformed absolute URLs, and pagination loops.

## Production requests

When the user asks for an unattended, resumable, scheduled, or large crawl, also read [references/production.md](references/production.md). Add only the controls relevant to that deployment; a one-page utility does not need a production stack.

## Deliver

Leave the user with the spider, its focused offline test, the chosen output mechanism, and the exact command to run it. Mention any part that could not be verified, especially selectors inferred without a representative response or JavaScript-rendered content that needs a different fetch client.
