from __future__ import annotations

import argparse
from collections.abc import AsyncIterator, Iterable, Iterator
from pathlib import Path
from typing import TYPE_CHECKING

from silkworm import HTMLResponse, Request, Response, Spider, run_spider
from silkworm.middlewares import RetryMiddleware, UserAgentMiddleware
from silkworm.pipelines import JsonLinesPipeline

if TYPE_CHECKING:
    from silkworm._types import MetaData
    from silkworm.logging import _Logger


class StartUrlsFromFileSpider(Spider):
    """
    Reads initial URLs from a text file and turns each URL into a Request.

    The input file should contain one URL per line. Empty lines and lines
    starting with "#" are ignored.
    """

    name = "start_urls_from_file"

    def __init__(
        self,
        urls_file: str,
        *,
        name: str | None = None,
        start_urls: Iterable[str] | None = None,
        custom_settings: MetaData | None = None,
        logger: _Logger | dict[str, object] | None = None,
    ) -> None:
        super().__init__(
            name=name,
            start_urls=start_urls,
            custom_settings=custom_settings,
            logger=logger,
        )
        self.urls_file = Path(urls_file)
        if not self.urls_file.exists():
            raise FileNotFoundError(f"URLs file not found: {self.urls_file}")

    def _iter_urls(self) -> Iterator[tuple[int, str]]:
        with self.urls_file.open("r", encoding="utf-8") as fp:
            for line_no, raw in enumerate(fp, 1):
                url = raw.strip()
                if not url or url.startswith("#"):
                    continue
                yield line_no, url

    async def start_requests(self) -> AsyncIterator[Request]:
        for line_no, url in self._iter_urls():
            yield Request(
                url=url,
                callback=self.parse_page,
                headers={"Accept": "text/html,application/xhtml+xml"},
                meta={"source_file": str(self.urls_file), "source_line": line_no},
                priority=line_no,
            )

    async def parse_page(self, response: Response):
        title = ""
        if isinstance(response, HTMLResponse):
            title_el = await response.select_first("title")
            title = title_el.text.strip() if title_el and title_el.text else ""

        yield {
            "url": response.url,
            "status": response.status,
            "title": title,
            "source_file": response.request.meta.get("source_file"),
            "source_line": response.request.meta.get("source_line"),
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read start URLs from a text file and fetch each page.",
    )
    parser.add_argument(
        "--urls-file",
        required=True,
        help="Text file with one URL per line. Blank lines and # comments are ignored.",
    )
    parser.add_argument(
        "--output",
        default="data/start_urls_from_file.jl",
        help="Output JSON Lines path.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_spider(
        StartUrlsFromFileSpider,
        urls_file=args.urls_file,
        request_middlewares=[UserAgentMiddleware()],
        response_middlewares=[RetryMiddleware(max_times=3)],
        item_pipelines=[JsonLinesPipeline(args.output, use_opendal=False)],
        concurrency=16,
        request_timeout=10,
        log_stats_interval=10,
    )


if __name__ == "__main__":
    main()
