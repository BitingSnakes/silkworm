"""Adaptable Silkworm spider scaffold."""

from __future__ import annotations

from silkworm import HTMLResponse, Response, Spider, run_spider
from silkworm.middlewares import RetryMiddleware, UserAgentMiddleware
from silkworm.pipelines import JsonLinesPipeline


class ExampleSpider(Spider):
    name = "example"
    start_urls = ("https://example.com/items/",)
    allowed_domains = ("example.com",)

    async def parse(self, response: Response) -> None:
        if not isinstance(response, HTMLResponse):
            return

        for card in await response.select(".item"):
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

        if (next_link := await response.select_first("a.next")) and (
            href := next_link.attr("href")
        ):
            await response.follow(href, callback=self.parse)


def main() -> None:
    run_spider(
        ExampleSpider,
        request_middlewares=[UserAgentMiddleware()],
        response_middlewares=[RetryMiddleware(max_times=3)],
        item_pipelines=[JsonLinesPipeline("data/items.jl")],
        concurrency=4,
        request_timeout=20,
        max_depth=10,
    )


if __name__ == "__main__":
    main()
