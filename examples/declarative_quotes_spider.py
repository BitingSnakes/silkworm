from __future__ import annotations


from silkworm import HTMLResponse, Response, Spider, run_spider
from silkworm.declarative import Attr, Item, Text
from silkworm.middlewares import RetryMiddleware, SkipNonHTMLMiddleware
from silkworm.pipelines import JsonLinesPipeline


class Quote(Item):
    __selector__ = ".quote"

    text: str = Text(".text", strip=True)
    author: str = Text(".author", strip=True)
    author_url: str = Attr("a[href*='/author/']", "href", absolute=True)
    tags: list[str] = Text(".tag", strip=True)


class DeclarativeQuotesSpider(Spider):
    name = "declarative-quotes"
    start_urls = ("https://quotes.toscrape.com/",)

    async def parse(
        self,
        response: Response,
    ):
        if not isinstance(response, HTMLResponse):
            return

        async for quote in Quote.extract(response):
            yield quote.to_dict()

        next_link = await response.select_first("li.next > a")
        if next_link is None:
            return
        href = next_link.attr("href")
        if href:
            yield response.follow(href, callback=self.parse)


if __name__ == "__main__":
    run_spider(
        DeclarativeQuotesSpider,
        response_middlewares=[
            SkipNonHTMLMiddleware(),
            RetryMiddleware(max_times=3),
        ],
        item_pipelines=[JsonLinesPipeline("data/declarative-quotes.jl")],
    )
