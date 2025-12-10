from __future__ import annotations

import argparse
from urllib.parse import urljoin

from pydantic import BaseModel, ValidationError, field_validator  # type: ignore[import]

from silkworm import HTMLResponse, Response, Spider, run_spider
from silkworm.logging import get_logger
from silkworm.middlewares import (
    DelayMiddleware,
    RequestMiddleware,
    ResponseMiddleware,
    RetryMiddleware,
    UserAgentMiddleware,
)
from silkworm.pipelines import ItemPipeline, JsonLinesPipeline


class HackerNewsPost(BaseModel):
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
    def validate_not_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be empty")
        return value

    @field_validator("age")
    @classmethod
    def clean_age(cls, value: str | None) -> str | None:
        return value.strip() if value else value


class HackerNewsSpider(Spider):
    name = "hacker_news_latest"
    start_urls = ("https://news.ycombinator.com/newest",)

    def __init__(self, pages: int = 5, **kwargs):
        super().__init__(**kwargs)
        self.pages_requested = max(1, pages)
        self.pages_seen = 0
        self.logger = get_logger(component="HackerNewsSpider", spider=self.name)

    async def parse(self, response: Response):
        if not isinstance(response, HTMLResponse):
            self.log.warning("Skipping non-HTML response", url=response.url)
            return

        html = response
        self.pages_seen += 1

        for row in await html.select("tr.athing"):
            post_id = row.attr("id")
            rank_el = row.select_first(".rank")
            rank = None
            if rank_el:
                rank_text = rank_el.text.replace(".", "").strip()
                rank = int(rank_text) if rank_text.isdigit() else None

            title_el = row.select_first("span.titleline a, a.storylink")
            title = title_el.text if title_el else ""
            href = title_el.attr("href") if title_el else ""
            url = urljoin(html.url, href)

            subtext = (
                await html.select_first(f"tr.athing[id='{post_id}'] + tr .subtext")
                if post_id
                else None
            )

            points = self._parse_points(subtext)
            comments = self._parse_comments(subtext)
            author_el = subtext.select_first("a.hnuser") if subtext else None
            author = author_el.text if author_el else None
            age_el = subtext.select_first(".age a") if subtext else None
            age = age_el.text if age_el else None

            try:
                item = HackerNewsPost(
                    title=title,
                    url=url,
                    author=author,
                    points=points,
                    comments=comments,
                    rank=rank,
                    age=age,
                    post_id=int(post_id) if post_id and post_id.isdigit() else None,
                )
                self.log.debug(
                    "Scraped story",
                    title=item.title,
                    points=item.points,
                    comments=item.comments,
                )
                yield item.model_dump()
            except ValidationError as exc:
                self.log.warning("Skipping invalid story", errors=exc.errors())
                continue

        more_link = await html.select_first("a.morelink")
        if more_link and self.pages_seen < self.pages_requested:
            href = more_link.attr("href")
            if href:
                yield html.follow(href, callback=self.parse)

    @staticmethod
    def _parse_points(subtext) -> int | None:
        if not subtext:
            return None
        score_el = subtext.select_first(".score")
        if not score_el or not score_el.text:
            return None
        value = score_el.text.split()[0]
        return int(value) if value.isdigit() else None

    @staticmethod
    def _parse_comments(subtext) -> int | None:
        if not subtext:
            return None
        for link in subtext.select("a"):
            text = link.text.strip().lower()
            if "comment" in text:
                first = text.split()[0]
                return int(first) if first.isdigit() else 0
        return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape latest Hacker News posts.")
    parser.add_argument(
        "--pages",
        type=int,
        default=5,
        help="Number of pagination pages to crawl (>=1).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    request_mw: list[RequestMiddleware] = [
        UserAgentMiddleware(),
        DelayMiddleware(min_delay=0.3, max_delay=1.0),  # Random delay
    ]
    response_mw: list[ResponseMiddleware] = [
        RetryMiddleware(max_times=3, sleep_http_codes=[403]),
    ]
    pipelines: list[ItemPipeline] = [
        JsonLinesPipeline("data/hackernews.jl"),
    ]

    run_spider(
        HackerNewsSpider,
        request_middlewares=request_mw,
        response_middlewares=response_mw,
        item_pipelines=pipelines,
        request_timeout=10,
        log_stats_interval=10,  # Log statistics every 10 seconds
        pages=args.pages,
    )


if __name__ == "__main__":
    main()
