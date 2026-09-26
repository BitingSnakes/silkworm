"""
Scrape the newest posts from Hacker News (news.ycombinator.com/newest).

This is a more realistic example than the quotes spiders. On Hacker News one
post is split across TWO table rows:

    <tr class="athing" id="123">      <- rank, title, link
    <tr> ... <td class="subtext">     <- points, author, age, comments

So for each post we read the title row and its matching "subtext" cell.

What you will learn:
- Combining data from neighbouring HTML elements.
- Turning text like "42 points" into the number 42.
- Being polite with DelayMiddleware (waits between requests).
- Limiting how many pages you crawl with a command-line option.

How to run:
    python examples/hackernews_spider.py            # 5 pages
    python examples/hackernews_spider.py --pages 2

Output:
    data/hackernews.jl
"""

from __future__ import annotations

import argparse

from pydantic import BaseModel, ValidationError, field_validator

from silkworm import HTMLResponse, Response, Spider, run_spider
from silkworm.middlewares import (
    DelayMiddleware,
    RetryMiddleware,
    UserAgentMiddleware,
)
from silkworm.pipelines import JsonLinesPipeline


class HackerNewsPost(BaseModel):
    """One Hacker News post. Fields that may be missing default to None."""

    title: str
    url: str
    author: str | None = None
    points: int | None = None
    comments: int | None = None
    rank: int | None = None
    age: str | None = None
    post_id: int | None = None

    @field_validator("title", "url")
    @classmethod
    def must_not_be_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be empty")
        return value


def first_number(text: str | None) -> int | None:
    """
    Return the first whole number found in `text`, or None.

    Examples: "42 points" -> 42, "3." -> 3, "discuss" -> None
    """
    if not text:
        return None
    words = text.split()
    if not words:
        return None
    digits = words[0].rstrip(".")
    return int(digits) if digits.isdigit() else None


class HackerNewsSpider(Spider):
    name = "hacker_news_latest"
    start_urls = ("https://news.ycombinator.com/newest",)

    def __init__(self, pages: int = 5, **kwargs) -> None:
        super().__init__(**kwargs)
        self.max_pages = max(1, pages)  # Always crawl at least 1 page.
        self.pages_seen = 0

    async def parse(self, response: Response):
        if not isinstance(response, HTMLResponse):
            self.log.warning("Skipping non-HTML response", url=response.url)
            return

        self.pages_seen += 1

        # The page lists all title rows and all subtext cells in the same order:
        # the 1st title row belongs to the 1st subtext cell, and so on.
        # zip() walks through both lists together, giving us one pair per post.
        title_rows = await response.select("tr.athing")
        subtext_cells = await response.select("td.subtext")

        for row, subtext in zip(title_rows, subtext_cells, strict=False):
            # --- Data from the title row ---
            post_id = row.attr("id")

            rank_el = await row.select_first(".rank")
            rank = first_number(rank_el.text) if rank_el else None

            title_el = await row.select_first("span.titleline a")
            title = title_el.text if title_el else ""
            href = title_el.attr("href") if title_el else None
            # Some links are relative (e.g. "item?id=1"), so make them absolute.
            url = response.url_join(href) if href else ""

            # --- Data from the subtext cell ---
            score_el = await subtext.select_first(".score")
            points = first_number(score_el.text) if score_el else None

            author_el = await subtext.select_first("a.hnuser")
            author = author_el.text if author_el else None

            age_el = await subtext.select_first(".age a")
            age = age_el.text.strip() if age_el else None

            # The comments link says "12 comments" or "discuss" (= 0).
            comments = None
            for link in await subtext.select("a"):
                if "comment" in link.text or link.text == "discuss":
                    comments = first_number(link.text) or 0
                    break

            try:
                post = HackerNewsPost(
                    title=title,
                    url=url,
                    author=author,
                    points=points,
                    comments=comments,
                    rank=rank,
                    age=age,
                    post_id=first_number(post_id),
                )
            except ValidationError as exc:
                self.log.warning("Skipping invalid post", errors=exc.errors())
                continue

            yield post.model_dump()

        # Follow the "More" link until we reach the page limit.
        if self.pages_seen >= self.max_pages:
            return

        more_link = await response.select_first("a.morelink")
        if more_link is not None:
            href = more_link.attr("href")
            if href:
                yield response.follow(href, callback=self.parse)


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape latest Hacker News posts.")
    parser.add_argument(
        "--pages",
        type=int,
        default=5,
        help="Number of pages to crawl (at least 1).",
    )
    args = parser.parse_args()

    run_spider(
        HackerNewsSpider(pages=args.pages),
        request_middlewares=[
            UserAgentMiddleware(),
            # Wait a random 0.3-1.0 seconds between requests to be polite.
            DelayMiddleware(min_delay=0.3, max_delay=1.0),
        ],
        response_middlewares=[
            # Also retry (after a pause) when the site answers 403 Forbidden.
            RetryMiddleware(max_times=3, sleep_http_codes=[403]),
        ],
        item_pipelines=[JsonLinesPipeline("data/hackernews.jl", use_opendal=False)],
        request_timeout=10,
        log_stats_interval=10,
    )


if __name__ == "__main__":
    main()
