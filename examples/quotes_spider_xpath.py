"""
Scrape quotes using XPath instead of CSS selectors.

CSS selectors and XPath are two ways to find elements in HTML. This file does
the same job as `quotes_spider.py`, so you can compare them side by side:

    CSS selector          XPath
    ------------------    -----------------------------------
    .quote                //div[@class='quote']
    .text                 .//span[@class='text']
    li.next > a           //li[@class='next']/a

XPath tips:
- `//` means "anywhere below". `//div` finds every <div> in the document.
- `.//` means "anywhere below THIS element" (use it inside a loop).
- `[@class='x']` filters by an attribute value.

How to run:
    python examples/quotes_spider_xpath.py

Output:
    data/quotes_xpath.jl
"""

from __future__ import annotations

from silkworm import HTMLResponse, Response, Spider, run_spider
from silkworm.middlewares import RetryMiddleware, UserAgentMiddleware
from silkworm.pipelines import JsonLinesPipeline


class QuotesSpiderXPath(Spider):
    name = "quotes_xpath"
    start_urls = ("https://quotes.toscrape.com/",)

    async def parse(self, response: Response):
        if not isinstance(response, HTMLResponse):
            self.log.warning("Skipping non-HTML response", url=response.url)
            return

        # Find every quote block on the page.
        for quote_el in await response.xpath("//div[@class='quote']"):
            # Search INSIDE this quote block, so start the XPath with ".//".
            text_el = await quote_el.xpath_first(".//span[@class='text']")
            author_el = await quote_el.xpath_first(".//small[@class='author']")
            tag_els = await quote_el.xpath(".//a[@class='tag']")

            if text_el is None or author_el is None:
                self.log.warning("Skipping quote with missing fields")
                continue

            yield {
                "text": text_el.text.strip(),
                "author": author_el.text.strip(),
                "tags": [tag.text.strip() for tag in tag_els],
            }

        # Find the "Next" link and follow it.
        next_link = await response.xpath_first("//li[@class='next']/a")
        if next_link is not None:
            href = next_link.attr("href")
            if href:
                self.log.info("Following next page", href=href)
                yield response.follow(href, callback=self.parse)


if __name__ == "__main__":
    run_spider(
        QuotesSpiderXPath,
        request_middlewares=[UserAgentMiddleware()],
        response_middlewares=[RetryMiddleware(max_times=3)],
        item_pipelines=[JsonLinesPipeline("data/quotes_xpath.jl", use_opendal=False)],
        request_timeout=10,
        log_stats_interval=10,
    )
