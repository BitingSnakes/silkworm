from __future__ import annotations

import argparse
from urllib.parse import urljoin

from pydantic import BaseModel, ValidationError, field_validator

from silkworm import HTMLResponse, Spider, run_spider
from silkworm.logging import get_logger
from silkworm.middlewares import (
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
    start_urls = ["https://news.ycombinator.com/newest"]

    def __init__(self, pages: int = 5, **kwargs):
        super().__init__(**kwargs)
        self.pages_requested = max(1, pages)
        self.pages_seen = 0
        self.logger = get_logger(component="HackerNewsSpider", spider=self.name)

    async def parse(self, response: HTMLResponse):
        self.pages_seen += 1

        for row in response.css("tr.athing"):
            post_id = row.attr("id")
            rank_el = row.find(".rank")
            rank = None
            if rank_el:
                rank_text = rank_el.text.replace(".", "").strip()
                rank = int(rank_text) if rank_text.isdigit() else None

            title_el = row.find("span.titleline a, a.storylink")
            title = title_el.text if title_el else ""
            href = title_el.attr("href") if title_el else ""
            url = urljoin(response.url, href)

            subtext = (
                response.find(f"tr.athing[id='{post_id}'] + tr .subtext")
                if post_id
                else None
            )

            points = self._parse_points(subtext)
            comments = self._parse_comments(subtext)
            author_el = subtext.find("a.hnuser") if subtext else None
            author = author_el.text if author_el else None
            age_el = subtext.find(".age a") if subtext else None
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
                self.logger.debug(
                    "Scraped story",
                    title=item.title,
                    points=item.points,
                    comments=item.comments,
                )
                yield (
                    item.model_dump() if hasattr(item, "model_dump") else item.dict()
                )
            except ValidationError as exc:
                self.logger.warning("Skipping invalid story", errors=exc.errors())
                continue

        more_link = response.find("a.morelink")
        if more_link and self.pages_seen < self.pages_requested:
            href = more_link.attr("href")
            if href:
                yield response.follow(href, callback=self.parse)

    @staticmethod
    def _parse_points(subtext) -> int | None:
        if not subtext:
            return None
        score_el = subtext.find(".score")
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
    ]
    response_mw: list[ResponseMiddleware] = [
        RetryMiddleware(max_times=3),
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
        pages=args.pages,
    )


if __name__ == "__main__":
    main()
