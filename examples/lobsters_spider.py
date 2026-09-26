"""
Scrape stories from the Lobsters front page (lobste.rs).

What you will learn:
- Reading many fields from one HTML block, where some fields may be missing.
- Using a regular expression to pull a number out of text ("12 comments" -> 12).
- Running a spider on uvloop, a faster event loop (Linux/macOS).
- Being polite with DelayMiddleware and retrying on "429 Too Many Requests".

How to run:
    python examples/lobsters_spider.py            # 1 page
    python examples/lobsters_spider.py --pages 3

Output:
    data/lobsters.jl
"""

from __future__ import annotations

import argparse
import re

from pydantic import BaseModel, ValidationError, field_validator

from silkworm import HTMLResponse, Response, Spider, run_spider_uvloop
from silkworm.middlewares import (
    DelayMiddleware,
    RetryMiddleware,
    UserAgentMiddleware,
)
from silkworm.pipelines import JsonLinesPipeline


class LobstersStory(BaseModel):
    """One Lobsters story. Fields that may be missing default to None."""

    title: str
    url: str
    short_id: str
    tags: list[str]
    author: str | None = None
    points: int | None = None
    comments: int | None = None
    age: str | None = None
    domain: str | None = None

    @field_validator("title", "url", "short_id")
    @classmethod
    def must_not_be_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be empty")
        return value


def extract_number(text: str | None) -> int | None:
    """
    Find the first number in `text`, or return None.

    Examples: "12 comments" -> 12, "1,024" -> 1024, "no comments" -> None
    """
    if not text:
        return None
    # \d+ means "one or more digits".
    match = re.search(r"\d+", text.replace(",", ""))
    if match is None:
        return None
    return int(match.group())


class LobstersSpider(Spider):
    name = "lobsters_front_page"
    start_urls = ("https://lobste.rs/",)

    def __init__(self, pages: int = 1, **kwargs) -> None:
        super().__init__(**kwargs)
        self.max_pages = max(1, pages)  # Always crawl at least 1 page.
        self.pages_seen = 0

    async def parse(self, response: Response):
        if not isinstance(response, HTMLResponse):
            self.log.warning("Skipping non-HTML response", url=response.url)
            return

        self.pages_seen += 1

        for story in await response.select("ol.stories > li.story"):
            # The story id is stored in data-shortid="abc123" (or id="story_abc123").
            short_id = story.attr("data-shortid") or story.attr("id") or ""
            short_id = short_id.removeprefix("story_")

            # Title and link.
            title_el = await story.select_first("span.link a.u-url")
            title = title_el.text if title_el else ""
            href = title_el.attr("href") if title_el else None
            url = response.url_join(href) if href else ""

            # Tags like "python", "rust".
            tags = [tag.text.strip() for tag in await story.select("span.tags a.tag")]

            # The remaining fields are optional: use None when they are missing.
            domain_el = await story.select_first("a.domain")
            author_el = await story.select_first(".byline .u-author")
            time_el = await story.select_first(".byline time")
            comments_el = await story.select_first(".comments_label a")
            points_el = await story.select_first(".voters .upvoter")

            try:
                story_item = LobstersStory(
                    title=title,
                    url=url,
                    short_id=short_id,
                    tags=tags,
                    domain=domain_el.text if domain_el else None,
                    author=author_el.text if author_el else None,
                    age=time_el.text.strip() if time_el else None,
                    comments=extract_number(comments_el.text) if comments_el else None,
                    points=extract_number(points_el.text) if points_el else None,
                )
            except ValidationError as exc:
                self.log.warning("Skipping invalid story", errors=exc.errors())
                continue

            yield story_item.model_dump()

        # Follow the "Page 2 >>" link until we reach the page limit.
        if self.pages_seen >= self.max_pages:
            return

        # There can be several links here ("<< Page 1 | Page 3 >>").
        # The last one always points forward.
        next_links = await response.select("div.morelink a[href]")
        if next_links:
            href = next_links[-1].attr("href")
            if href:
                yield response.follow(href, callback=self.parse)


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Lobsters front page stories.")
    parser.add_argument(
        "--pages",
        type=int,
        default=1,
        help="Number of pages to crawl (at least 1).",
    )
    args = parser.parse_args()

    # run_spider_uvloop works like run_spider but uses the faster uvloop.
    # Install it with: pip install "silkworm-rs[uvloop]"
    run_spider_uvloop(
        LobstersSpider(pages=args.pages),
        request_middlewares=[
            UserAgentMiddleware(),
            # Wait a random 0.3-1.0 seconds between requests to be polite.
            DelayMiddleware(min_delay=0.3, max_delay=1.0),
        ],
        response_middlewares=[
            # Lobsters may answer 429 "Too Many Requests" if we are too fast.
            # Pause and retry (up to 15 times) when that happens.
            RetryMiddleware(max_times=15, sleep_http_codes=[403, 429]),
        ],
        item_pipelines=[JsonLinesPipeline("data/lobsters.jl", use_opendal=False)],
        request_timeout=10,
        log_stats_interval=10,
        concurrency=32,
    )


if __name__ == "__main__":
    main()
