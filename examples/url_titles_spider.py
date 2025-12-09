from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from silkworm import HTMLResponse, Response, Spider, run_spider
from silkworm.logging import get_logger
from silkworm.middlewares import (
    RequestMiddleware,
    ResponseMiddleware,
    RetryMiddleware,
    UserAgentMiddleware,
)
from silkworm.pipelines import ItemPipeline, JsonLinesPipeline
from silkworm.request import Request


class UrlTitlesSpider(Spider):
    """
    Reads a JSON Lines file containing {"url": "..."} objects and fetches each page title.
    Extra fields on each line are preserved and passed through to the output.
    """

    name = "url_titles_from_file"

    def __init__(self, urls_file: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.urls_path = Path(urls_file)
        self.logger = get_logger(component="UrlTitlesSpider", spider=self.name)
        self.records = self._load_records(self.urls_path)

    def _load_records(self, path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            raise FileNotFoundError(f"URLs file not found: {path}")

        records: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as fp:
            for line_no, raw in enumerate(fp, 1):
                line = raw.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                except json.JSONDecodeError as exc:
                    self.logger.warning(
                        "Skipping invalid JSON line",
                        line_number=line_no,
                        error=str(exc),
                    )
                    continue

                if not isinstance(data, dict):
                    self.logger.warning(
                        "Skipping non-object JSON line", line_number=line_no
                    )
                    continue

                url = str(data.get("url", "")).strip()
                if not url:
                    self.logger.warning(
                        "Skipping line without url field", line_number=line_no
                    )
                    continue

                data["url"] = url
                records.append(data)

        self.logger.info("Loaded URLs", count=len(records), path=str(path))
        return records

    async def start_requests(self):
        for record in self.records:
            yield Request(
                url=record["url"], callback=self.parse, meta={"record": record}
            )

    async def parse(self, response: Response):
        record: dict[str, Any] = response.request.meta.get("record", {})

        html_response: HTMLResponse | None
        if isinstance(response, HTMLResponse):
            html_response = response
        elif "<html" in response.text.lower():
            # Some servers omit/lie about content-type; fall back to HTML parsing.
            html_response = HTMLResponse(
                url=response.url,
                status=response.status,
                headers=response.headers,
                body=response.body,
                request=response.request,
            )
        else:
            html_response = None
            self.logger.debug(
                "Non-HTML response received",
                url=response.url,
                content_type=response.headers.get("content-type"),
            )

        page_title = ""
        if html_response:
            title_el = html_response.find("title")
            if title_el and title_el.text:
                page_title = title_el.text.strip()

        item = {
            **record,
            "page_title": page_title,
            "final_url": response.url,
            "status": response.status,
        }
        self.logger.debug("Scraped title", url=response.url, title=page_title)
        yield item


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch page titles for URLs listed in a JSONL file."
    )
    parser.add_argument(
        "--urls-file",
        type=str,
        required=True,
        help="Path to a JSON Lines file with one object per line that includes a 'url' field.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/url_titles.jl",
        help="Where to write scraped results (JSON Lines).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    request_mw: list[RequestMiddleware] = [
        UserAgentMiddleware(),
    ]
    response_mw: list[ResponseMiddleware] = [
        RetryMiddleware(max_times=3, sleep_http_codes=[403, 429]),
    ]
    pipelines: list[ItemPipeline] = [
        JsonLinesPipeline(args.output),
    ]

    run_spider(
        UrlTitlesSpider,
        concurrency=64,
        request_middlewares=request_mw,
        response_middlewares=response_mw,
        item_pipelines=pipelines,
        request_timeout=5,
        urls_file=args.urls_file,
    )


if __name__ == "__main__":
    main()
