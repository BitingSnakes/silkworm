from __future__ import annotations

import argparse
import re
from urllib.parse import urljoin

from pydantic import BaseModel, ValidationError, field_validator  # type: ignore[import]

from silkworm import HTMLResponse, Response, Spider, run_spider_uvloop
from silkworm.logging import get_logger
from silkworm.middlewares import (
    DelayMiddleware,
    RequestMiddleware,
    ResponseMiddleware,
    RetryMiddleware,
    UserAgentMiddleware,
)
from silkworm.pipelines import ItemPipeline, JsonLinesPipeline


class LobstersStory(BaseModel):
    title: str
    url: str
    author: str | None = None
    points: int | None = None
    comments: int | None = None
    tags: list[str]
    short_id: str
    age: str | None = None
    domain: str | None = None

    @field_validator("title", "url", "short_id")
    @classmethod
    def validate_not_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be empty")
        return value

    @field_validator("tags")
    @classmethod
    def clean_tags(cls, value: list[str]) -> list[str]:
        return [tag.strip() for tag in value if tag.strip()]

    @field_validator("age")
    @classmethod
    def clean_age(cls, value: str | None) -> str | None:
        return value.strip() if value else value


class LobstersSpider(Spider):
    name = "lobsters_front_page"
    start_urls = ("https://lobste.rs/",)

    def __init__(self, pages: int = 1, **kwargs):
        super().__init__(**kwargs)
        self.pages_requested = max(1, pages)
        self.pages_seen = 0
        self.logger = get_logger(component="LobstersSpider", spider=self.name)

    async def parse(self, response: Response):
        if not isinstance(response, HTMLResponse):
            self.log.warning("Skipping non-HTML response", url=response.url)
            return

        html = response
        self.pages_seen += 1

        for story in await html.select("ol.stories > li.story"):
            short_id = story.attr("data-shortid") or story.attr("id") or ""
            short_id = short_id.replace("story_", "")

            title_el = story.select_first("span.link a.u-url")
            title = title_el.text if title_el else ""
            href = title_el.attr("href") if title_el else ""
            url = urljoin(html.url, href)

            domain_el = story.select_first("a.domain")
            domain = domain_el.text if domain_el else None

            tags = [tag.text for tag in story.select("span.tags a.tag")]

            author_el = story.select_first(".byline .u-author")
            author = author_el.text if author_el else None

            time_el = story.select_first(".byline time")
            age = time_el.text if time_el else None

            comments_el = story.select_first(".comments_label a")
            comments_text = comments_el.text.strip() if comments_el else ""
            comments = self._extract_number(comments_text)

            points_el = story.select_first(".voters .upvoter")
            points = self._extract_number(points_el.text if points_el else None)

            try:
                item = LobstersStory(
                    title=title,
                    url=url,
                    author=author,
                    points=points,
                    comments=comments,
                    tags=tags,
                    short_id=short_id,
                    age=age,
                    domain=domain,
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

        next_links = await html.select("div.morelink a[href]")
        if len(next_links) > 0 and self.pages_seen < self.pages_requested:
            next_link = next_links[-1]
            href = next_link.attr("href")
            if href:
                yield html.follow(href, callback=self.parse)

    @staticmethod
    def _extract_number(text: str | None) -> int | None:
        if not text:
            return None
        match = re.search(r"\d+", text.replace(",", ""))
        return int(match.group()) if match else None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Lobsters front page stories.")
    parser.add_argument(
        "--pages",
        type=int,
        default=1,
        help="Number of pagination pages to crawl (>=1).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    request_mw: list[RequestMiddleware] = [
        UserAgentMiddleware(),
        DelayMiddleware(min_delay=0.3, max_delay=1.0),  # Randomized polite delay
    ]
    response_mw: list[ResponseMiddleware] = [
        RetryMiddleware(max_times=15, sleep_http_codes=[403, 429]),
    ]
    pipelines: list[ItemPipeline] = [
        JsonLinesPipeline("data/lobsters.jl"),
    ]

    run_spider_uvloop(
        LobstersSpider,
        request_middlewares=request_mw,
        response_middlewares=response_mw,
        item_pipelines=pipelines,
        request_timeout=10,
        log_stats_interval=10,
        concurrency=32,
        pages=args.pages,
    )


if __name__ == "__main__":
    main()
