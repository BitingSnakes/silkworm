"""
Write logs to two places at once: readable text on screen, JSON in a file.

- The console gets colored, human-friendly lines (nice while developing).
- The file gets one JSON object per line (easy to search, filter, or load into
  log tools later).

Without arguments the script just configures the logger and writes a few
sample messages. With `--run` it crawls a few pages of quotes and then reads
the JSON log file back to show a small summary.

How to run:
    python examples/hybrid_logger_demo.py        # quick demo, no crawling
    python examples/hybrid_logger_demo.py --run  # crawl + analyse the log file

Output:
    data/hybrid_spider_logs.jsonl
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

from silkworm import HTMLResponse, Response, Spider, run_spider
from silkworm.logging import get_logger

LOG_FILE = Path("data/hybrid_spider_logs.jsonl")


def configure_hybrid_logger(log_file: Path, level: str = "INFO") -> None:
    """Send logs to the console (as text) AND to `log_file` (as JSON)."""
    # Make sure the data/ folder exists.
    log_file.parent.mkdir(parents=True, exist_ok=True)

    # A logger can have several "handlers". Each handler sends logs somewhere.
    get_logger().configure(
        handlers=[
            # Handler 1: colored text on the screen.
            {"sink": "stderr", "level": level, "colorize": True},
            # Handler 2: JSON lines in a file ("serialize" means "write as JSON").
            {"sink": str(log_file), "level": level, "serialize": True},
        ],
    )
    print(f"Logger configured: text on screen, JSON in {log_file}\n")


class HybridLoggerSpider(Spider):
    name = "hybrid_logger_demo"
    start_urls = ("https://quotes.toscrape.com/",)
    max_pages = 3

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.pages_seen = 0
        self.quotes_count = 0

    async def parse(self, response: Response):
        if not isinstance(response, HTMLResponse):
            self.log.warning("Skipping non-HTML response", url=response.url)
            return

        self.pages_seen += 1
        self.log.info("Parsing quotes page", url=response.url, page=self.pages_seen)

        for quote_el in await response.select(".quote"):
            text_el = await quote_el.select_first(".text")
            author_el = await quote_el.select_first(".author")
            if text_el is None or author_el is None:
                continue

            self.quotes_count += 1
            # Extra keyword arguments become separate fields in the JSON log.
            self.log.debug(
                "Scraped quote",
                quote_number=self.quotes_count,
                author=author_el.text.strip(),
            )
            yield {"text": text_el.text.strip(), "author": author_el.text.strip()}

        if self.pages_seen >= self.max_pages:
            self.log.info("Reached page limit", total_quotes=self.quotes_count)
            return

        next_link = await response.select_first("li.next > a")
        if next_link is not None:
            href = next_link.attr("href")
            if href:
                yield response.follow(href, callback=self.parse)


def summarize_log_file(log_file: Path) -> None:
    """Read the JSON log file back and print a short summary."""
    if not log_file.exists():
        print(f"No log file found at {log_file}")
        return

    entries = []
    with log_file.open("r", encoding="utf-8") as file:
        for line in file:
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                pass  # Skip lines that aren't valid JSON.

    print(f"\nTotal JSON log entries: {len(entries)}")

    # Counter counts how many times each level appears.
    levels = Counter(entry.get("level", "UNKNOWN") for entry in entries)
    print("Entries by level:")
    for level, count in sorted(levels.items()):
        print(f"  {level}: {count}")

    print("\nFirst 3 entries:")
    for entry in entries[:3]:
        print(f"  {entry.get('level')} - {entry.get('message')}")
        print(f"    fields: {', '.join(entry.keys())}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Hybrid (text + JSON) logging demo.")
    parser.add_argument(
        "--run", action="store_true", help="Crawl a few pages and analyse the logs."
    )
    args = parser.parse_args()

    configure_hybrid_logger(LOG_FILE)

    if not args.run:
        # Quick demo: just write a few messages.
        logger = get_logger(component="Demo", environment="development")
        logger.info("This line goes to the screen and to the JSON file")
        logger.warning("Extra fields are stored as JSON keys", answer=42)
        summarize_log_file(LOG_FILE)
        print("\nTo crawl real pages, run: python examples/hybrid_logger_demo.py --run")
        return

    # `logger={...}` adds these fields to every log line from the spider.
    spider = HybridLoggerSpider(logger={"component": "QuotesSpider", "mode": "hybrid"})
    run_spider(spider, concurrency=8, request_timeout=10)
    summarize_log_file(LOG_FILE)


if __name__ == "__main__":
    main()
