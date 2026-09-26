"""
Crawl every page listed in a sitemap.xml and collect its SEO metadata.

A sitemap is an XML file where a website lists its pages:

    <urlset>
      <url><loc>https://example.com/page-1</loc></url>
      <url><loc>https://example.com/page-2</loc></url>
    </urlset>

Big sites often use a "sitemap index" that points to more sitemaps:

    <sitemapindex>
      <sitemap><loc>https://example.com/sitemap-posts.xml</loc></sitemap>
    </sitemapindex>

This spider handles both. For every page it collects:
- <title> and <link rel="canonical">
- <meta name="description">, keywords, author, robots, viewport
- Open Graph tags (<meta property="og:...">) used by Facebook, LinkedIn, ...
- Twitter Card tags (<meta name="twitter:...">)

What you will learn:
- Using two different callbacks: `parse_sitemap` for XML, `parse_page` for HTML.
- Parsing XML with rxml.
- Using a dict to map HTML tag names to output field names.

How to run:
    python examples/sitemap_spider.py --sitemap-url https://example.com/sitemap.xml
    python examples/sitemap_spider.py --sitemap-url https://example.com/sitemap.xml --pages 50

Output:
    data/sitemap_meta.jl  (change it with --output)
"""

from __future__ import annotations

import argparse
import re

import rxml

from silkworm import HTMLResponse, Request, Response, Spider, run_spider_uvloop
from silkworm.middlewares import (
    DelayMiddleware,
    RequestMiddleware,
    RetryMiddleware,
    SkipNonHTMLMiddleware,
    UserAgentMiddleware,
)
from silkworm.pipelines import JsonLinesPipeline

# Which <meta> tags to collect, and the output field name for each one.
# The key is the tag's `name` or `property` attribute (lowercase).
META_FIELDS = {
    # Standard HTML meta tags
    "description": "meta_description",
    "keywords": "meta_keywords",
    "author": "author",
    "robots": "robots",
    "viewport": "viewport",
    # Open Graph tags
    "og:title": "og_title",
    "og:description": "og_description",
    "og:type": "og_type",
    "og:url": "og_url",
    "og:image": "og_image",
    "og:site_name": "og_site_name",
    "og:locale": "og_locale",
    # Twitter Card tags
    "twitter:card": "twitter_card",
    "twitter:title": "twitter_title",
    "twitter:description": "twitter_description",
    "twitter:image": "twitter_image",
    "twitter:site": "twitter_site",
}


def find_loc_urls(parent, child_tag: str) -> list[str]:
    """
    Return the <loc> URL inside every <child_tag> element under `parent`.

    For example, find_loc_urls(root, "url") returns the page URLs of a sitemap.
    """
    urls = []
    for element in parent.search_by_name(child_tag):
        loc_nodes = element.search_by_name("loc")
        if loc_nodes and loc_nodes[0].text:
            urls.append(loc_nodes[0].text.strip())
    return urls


class SitemapSpider(Spider):
    name = "sitemap_metadata"

    def __init__(self, sitemap_url: str, max_pages: int | None = None, **kwargs):
        """
        Args:
            sitemap_url: URL of the sitemap.xml file.
            max_pages: Stop after this many pages (None = no limit).
        """
        super().__init__(**kwargs)
        self.sitemap_url = sitemap_url
        self.max_pages = max_pages
        self.pages_requested = 0

    async def start_requests(self):
        self.log.info("Fetching sitemap", url=self.sitemap_url)
        yield self.sitemap_request(self.sitemap_url)

    def sitemap_request(self, url: str) -> Request:
        """Build a request for a sitemap file."""
        return Request(
            url=url,
            callback=self.parse_sitemap,
            dont_filter=True,
            # Sitemaps are XML, not HTML. This tells SkipNonHTMLMiddleware
            # to let this response through anyway.
            meta={"allow_non_html": True},
        )

    async def parse_sitemap(self, response: Response):
        """Read a sitemap and request every page (or sub-sitemap) in it."""
        if response.status != 200:
            self.log.error(
                "Failed to fetch sitemap", url=response.url, status=response.status
            )
            return

        # rxml needs to know the name of the root tag ("urlset" or "sitemapindex").
        # We find it with a regular expression: the first "<name" in the file.
        # [a-zA-Z] skips "<?xml ...?>" and "<!-- comments -->".
        match = re.search(r"<([a-zA-Z][\w-]*?)[\s>]", response.text)
        if match is None:
            self.log.error("Could not find the root tag in sitemap", url=response.url)
            return

        try:
            root = rxml.read_string(response.text, match.group(1))
        except ValueError as exc:
            self.log.error(
                "Failed to parse sitemap XML", url=response.url, error=str(exc)
            )
            return

        # Case 1: a sitemap index. Request each sub-sitemap with this same method.
        sub_sitemaps = find_loc_urls(root, "sitemap")
        if sub_sitemaps:
            self.log.info("Found sitemap index", sub_sitemaps=len(sub_sitemaps))
            for url in sub_sitemaps:
                yield self.sitemap_request(url)
            return

        # Case 2: a normal sitemap. Request each page with parse_page().
        page_urls = find_loc_urls(root, "url")
        self.log.info("Found URLs in sitemap", count=len(page_urls), url=response.url)

        for url in page_urls:
            if self.max_pages is not None and self.pages_requested >= self.max_pages:
                self.log.info("Reached max pages limit", max_pages=self.max_pages)
                return

            self.pages_requested += 1
            yield Request(
                url=url,
                callback=self.parse_page,
                dont_filter=True,
                # Remember the sitemap URL; the final URL can differ after redirects.
                meta={"sitemap_url": url},
            )

    async def parse_page(self, response: Response):
        """Collect the title and meta tags of one HTML page."""
        if not isinstance(response, HTMLResponse):
            self.log.warning("Skipping non-HTML response", url=response.url)
            return

        item: dict[str, str | int] = {
            "url": str(response.request.meta.get("sitemap_url", response.url)),
            "final_url": response.url,
            "status": response.status,
        }

        title_el = await response.select_first("title")
        if title_el is not None and title_el.text.strip():
            item["title"] = title_el.text.strip()

        canonical_el = await response.select_first('link[rel="canonical"]')
        canonical_href = canonical_el.attr("href") if canonical_el else None
        if canonical_href:
            item["canonical_url"] = canonical_href.strip()

        # Look at every <meta> tag and keep the ones listed in META_FIELDS.
        for meta in await response.select("meta"):
            # Open Graph uses `property=`, most others use `name=`.
            tag_name = meta.attr("name") or meta.attr("property")
            content = meta.attr("content")
            if not tag_name or not content:
                continue

            field_name = META_FIELDS.get(tag_name.lower())
            if field_name is not None:
                item[field_name] = content.strip()

        yield item


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape metadata and Open Graph tags from pages in a sitemap."
    )
    parser.add_argument(
        "--sitemap-url", required=True, help="URL of the sitemap.xml file to process."
    )
    parser.add_argument(
        "--output",
        default="data/sitemap_meta.jl",
        help="Output file path (JSON Lines format).",
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=None,
        help="Maximum number of pages to scrape (default: no limit).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=16,
        help="Number of pages to download at the same time (default: 16).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Seconds to wait between requests (default: 0).",
    )
    args = parser.parse_args()

    request_middlewares: list[RequestMiddleware] = [UserAgentMiddleware()]
    if args.delay > 0:
        request_middlewares.append(DelayMiddleware(delay=args.delay))

    run_spider_uvloop(
        SitemapSpider(sitemap_url=args.sitemap_url, max_pages=args.pages),
        request_middlewares=request_middlewares,
        response_middlewares=[
            RetryMiddleware(max_times=3, sleep_http_codes=[403, 429, 503]),
            SkipNonHTMLMiddleware(),
        ],
        item_pipelines=[JsonLinesPipeline(args.output, use_opendal=False)],
        concurrency=args.concurrency,
        request_timeout=30,
        log_stats_interval=10,
        html_max_size_bytes=2_000_000,  # Only parse the first 2 MB of HTML.
    )


if __name__ == "__main__":
    main()
