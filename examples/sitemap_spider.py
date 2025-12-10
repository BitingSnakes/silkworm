"""
Example spider that loads a sitemap file and extracts meta + Open Graph information from pages.

This spider demonstrates:
- Parsing XML sitemap files to extract URLs using rxml
- Extracting HTML meta tags (title, description, keywords)
- Extracting Open Graph (OG) tags for social media metadata
- Using Pydantic for data validation
- Handling both sitemap.xml and sitemap index files

Usage:
    python examples/sitemap_spider.py --sitemap-url https://example.com/sitemap.xml
    python examples/sitemap_spider.py --sitemap-url https://example.com/sitemap.xml --output data/sitemap_meta.jl --pages 50
"""

from __future__ import annotations

import argparse
import asyncio
import re
from typing import Any, cast

import rxml
from pydantic import BaseModel, field_validator

from silkworm import HTMLResponse, Response, Spider, run_spider_uvloop
from silkworm.logging import get_logger
from silkworm.middlewares import (
    DelayMiddleware,
    RequestMiddleware,
    ResponseMiddleware,
    RetryMiddleware,
    SkipNonHTMLMiddleware,
    UserAgentMiddleware,
)
from silkworm.pipelines import ItemPipeline, JsonLinesPipeline
from silkworm.request import Request


class PageMetadata(BaseModel):
    """Structured page metadata including meta and Open Graph tags."""

    url: str
    final_url: str | None = None
    status: int | None = None

    # Standard HTML meta tags
    title: str | None = None
    meta_description: str | None = None
    meta_keywords: str | None = None
    canonical_url: str | None = None

    # Open Graph tags
    og_title: str | None = None
    og_description: str | None = None
    og_type: str | None = None
    og_url: str | None = None
    og_image: str | None = None
    og_site_name: str | None = None
    og_locale: str | None = None

    # Twitter Card tags
    twitter_card: str | None = None
    twitter_title: str | None = None
    twitter_description: str | None = None
    twitter_image: str | None = None
    twitter_site: str | None = None

    # Additional metadata
    author: str | None = None
    robots: str | None = None
    viewport: str | None = None

    @field_validator("url", "final_url", "canonical_url", "og_url")
    @classmethod
    def validate_url(cls, value: str | None) -> str | None:
        if value:
            return value.strip()
        return value


class SitemapSpider(Spider):
    """
    Spider that reads a sitemap.xml file and extracts metadata and Open Graph tags from each page.
    """

    name = "sitemap_metadata"

    def __init__(
        self,
        sitemap_url: str,
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize the sitemap spider.

        Args:
            sitemap_url: URL of the sitemap.xml file
            max_pages: Maximum number of pages to scrape from sitemap (None = unlimited)
            **kwargs: Additional arguments passed to Spider
        """
        super().__init__(**kwargs)
        self.sitemap_url = sitemap_url
        self.max_pages = max_pages
        self.pages_scraped = 0
        self._pages_lock = asyncio.Lock()  # Protect counter from concurrent access
        self.logger = get_logger(component="SitemapSpider", spider=self.name)

    async def start_requests(self):
        """Start by fetching the sitemap file."""
        self.log.info("Fetching sitemap", url=self.sitemap_url)
        yield Request(
            url=self.sitemap_url,
            callback=self.parse_sitemap,
            dont_filter=True,
            meta={"allow_non_html": True},  # allow XML through SkipNonHTMLMiddleware
        )

    async def parse_sitemap(self, response: Response):
        """
        Parse the sitemap XML and extract URLs.
        Handles both regular sitemaps and sitemap index files.
        """
        if response.status != 200:
            self.log.error(
                "Failed to fetch sitemap",
                url=response.url,
                status=response.status,
            )
            return

        try:
            # Detect root tag from XML content (skip XML declaration and comments)
            xml_text = response.text
            # Find first non-declaration, non-comment tag
            root_tag_match = re.search(r"<([a-zA-Z][\w-]*?)[\s>]", xml_text)
            if not root_tag_match:
                self.log.error(
                    "Could not detect root tag in sitemap",
                    url=response.url,
                )
                return

            root_tag = root_tag_match.group(1)
            root = cast(Any, rxml.read_string(xml_text, root_tag))

            # Check if this is a sitemap index (contains other sitemaps)
            sitemap_elements = root.search_by_name("sitemap")
            if sitemap_elements:
                self.log.info(
                    "Found sitemap index with sub-sitemaps",
                    count=len(sitemap_elements),
                )
                for sitemap_elem in sitemap_elements:
                    loc_nodes = sitemap_elem.search_by_name("loc")
                    if loc_nodes and len(loc_nodes) > 0 and loc_nodes[0].text:
                        self.log.debug("Following sub-sitemap", url=loc_nodes[0].text)
                        yield Request(
                            url=loc_nodes[0].text.strip(),
                            callback=self.parse_sitemap,
                            dont_filter=True,
                            meta={"allow_non_html": True},
                        )
                return

            # Otherwise, extract URLs from the sitemap
            url_elements = root.search_by_name("url")

            if not url_elements:
                self.log.warning(
                    "No URLs found in sitemap",
                    url=response.url,
                )
                return

            self.log.info(
                "Found URLs in sitemap",
                count=len(url_elements),
                url=response.url,
            )

            # Extract and request each URL
            for url_elem in url_elements:
                loc_nodes = url_elem.search_by_name("loc")
                if loc_nodes and len(loc_nodes) > 0 and loc_nodes[0].text:
                    url = loc_nodes[0].text.strip()

                    # Check if we've reached the max pages limit (with lock for concurrent safety)
                    async with self._pages_lock:
                        if (
                            self.max_pages is not None
                            and self.pages_scraped >= self.max_pages
                        ):
                            self.log.info(
                                "Reached max pages limit",
                                max_pages=self.max_pages,
                            )
                            return

                        self.pages_scraped += 1

                    yield Request(
                        url=url,
                        callback=self.parse_page,
                        dont_filter=True,
                    )

        except Exception as exc:
            self.log.error(
                "Failed to parse sitemap XML",
                url=response.url,
                error=str(exc),
            )

    async def parse_page(self, response: Response):
        """Extract metadata and Open Graph tags from the page."""
        if not isinstance(response, HTMLResponse):
            self.log.warning(
                "Skipping non-HTML response",
                url=response.url,
                content_type=response.headers.get("content-type"),
            )
            return

        html = response

        # Extract standard HTML metadata
        title_elem = await html.select_first("title")
        title = title_elem.text.strip() if title_elem and title_elem.text else None

        # Extract meta tags
        meta_tags = await html.select("meta")
        meta_data = {}

        for meta in meta_tags:
            name = meta.attr("name") or meta.attr("property")
            content = meta.attr("content")

            if name and content:
                name_lower = name.lower()

                # Standard meta tags
                if name_lower == "description":
                    meta_data["meta_description"] = content.strip()
                elif name_lower == "keywords":
                    meta_data["meta_keywords"] = content.strip()
                elif name_lower == "author":
                    meta_data["author"] = content.strip()
                elif name_lower == "robots":
                    meta_data["robots"] = content.strip()
                elif name_lower == "viewport":
                    meta_data["viewport"] = content.strip()

                # Open Graph tags
                elif name_lower == "og:title":
                    meta_data["og_title"] = content.strip()
                elif name_lower == "og:description":
                    meta_data["og_description"] = content.strip()
                elif name_lower == "og:type":
                    meta_data["og_type"] = content.strip()
                elif name_lower == "og:url":
                    meta_data["og_url"] = content.strip()
                elif name_lower == "og:image":
                    meta_data["og_image"] = content.strip()
                elif name_lower == "og:site_name":
                    meta_data["og_site_name"] = content.strip()
                elif name_lower == "og:locale":
                    meta_data["og_locale"] = content.strip()

                # Twitter Card tags
                elif name_lower == "twitter:card":
                    meta_data["twitter_card"] = content.strip()
                elif name_lower == "twitter:title":
                    meta_data["twitter_title"] = content.strip()
                elif name_lower == "twitter:description":
                    meta_data["twitter_description"] = content.strip()
                elif name_lower == "twitter:image":
                    meta_data["twitter_image"] = content.strip()
                elif name_lower == "twitter:site":
                    meta_data["twitter_site"] = content.strip()

        # Extract canonical URL
        canonical_elem = await html.select_first('link[rel="canonical"]')
        canonical_url = (
            canonical_elem.attr("href").strip()
            if canonical_elem and canonical_elem.attr("href")
            else None
        )

        # Create structured item
        original_url = response.request.meta.get("original_url")
        meta_url = original_url if isinstance(original_url, str) else response.url

        item = PageMetadata(
            url=meta_url,
            final_url=response.url,
            status=response.status,
            title=title,
            canonical_url=canonical_url,
            **meta_data,
        )

        self.log.debug(
            "Scraped page metadata",
            url=response.url,
            title=item.title,
            has_og=bool(item.og_title or item.og_description),
        )

        yield item.model_dump(exclude_none=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape metadata and Open Graph tags from pages in a sitemap."
    )
    parser.add_argument(
        "--sitemap-url",
        type=str,
        required=True,
        help="URL of the sitemap.xml file to process.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/sitemap_meta.jl",
        help="Output file path for scraped metadata (JSON Lines format).",
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=None,
        help="Maximum number of pages to scrape from the sitemap (default: unlimited).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=16,
        help="Number of concurrent requests (default: 16).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Delay between requests in seconds (default: 0.0).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    request_mw: list[RequestMiddleware] = [
        UserAgentMiddleware(),
    ]

    # Add delay middleware if specified
    if args.delay > 0:
        request_mw.append(DelayMiddleware(delay=args.delay))

    response_mw: list[ResponseMiddleware] = [
        RetryMiddleware(max_times=3, sleep_http_codes=[403, 429, 503]),
        SkipNonHTMLMiddleware(),
    ]

    pipelines: list[ItemPipeline] = [
        JsonLinesPipeline(args.output, use_opendal=True),
    ]

    run_spider_uvloop(
        SitemapSpider,
        request_middlewares=request_mw,
        response_middlewares=response_mw,
        item_pipelines=pipelines,
        concurrency=args.concurrency,
        request_timeout=30,
        log_stats_interval=10,
        html_max_size_bytes=2_000_000,  # 2MB limit for HTML parsing
        sitemap_url=args.sitemap_url,
        max_pages=args.pages,
    )


if __name__ == "__main__":
    main()
