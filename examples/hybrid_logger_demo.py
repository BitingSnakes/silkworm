"""
Example demonstrating hybrid logger configuration with Silkworm spiders.

This example shows how to configure a logger that:
- Streams human-readable textual logs to the console
- Writes structured JSON logs to a file for later analysis

This is useful for:
- Development: See readable logs in console
- Production: Parse JSON logs with log aggregation tools
- Debugging: Search and filter structured log data
"""

from __future__ import annotations

import os
from pathlib import Path

from logly import logger as logly_logger

from silkworm import HTMLResponse, Response, Spider, run_spider
from silkworm.logging import get_logger


def configure_hybrid_logger(
    json_log_file: str = "data/spider_logs.jsonl",
    log_level: str = "INFO",
) -> None:
    """
    Configure a hybrid logger that outputs:
    - Human-readable text to console (with colors)
    - Structured JSON to a file (for parsing/analysis)

    Args:
        json_log_file: Path to JSON log file
        log_level: Minimum log level (DEBUG, INFO, WARNING, ERROR)
    """
    # Ensure log directory exists
    log_path = Path(json_log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # Configure logly with hybrid output
    logly_logger.configure(
        level=log_level.upper(),
        # Console output: human-readable text
        console=True,
        json=False,  # Console gets text, not JSON
        color=True,  # Colorize console output
        show_time=True,
        show_module=True,
        show_function=False,
        # File output: structured JSON
        auto_sink=True,
        auto_sink_levels={
            log_level.upper(): {
                "path": json_log_file,
                "json": True,  # File gets JSON format
            }
        },
    )

    print("✓ Hybrid logger configured:")
    print("  - Console: Human-readable text with colors")
    print(f"  - File: Structured JSON at {json_log_file}")
    print()


class HybridLoggerSpider(Spider):
    """
    Spider demonstrating hybrid logger usage.

    Logs will appear:
    - In console as colored, readable text
    - In JSON file as structured data for analysis
    """

    name = "hybrid_logger_demo"
    start_urls = ("https://quotes.toscrape.com/",)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Logger can be configured via parameter or manually
        if self.logger is None:
            self.logger = get_logger(component="HybridSpider", spider=self.name)
        self.quotes_count = 0

    async def parse(self, response: Response):
        if not isinstance(response, HTMLResponse):
            if self.logger:
                self.logger.warning(
                    "Skipping non-HTML response",
                    url=response.url,
                    content_type=response.headers.get("content-type", "unknown"),
                )
            return

        html = response

        if self.logger:
            self.logger.info(
                "Parsing quotes page", url=html.url, quotes_so_far=self.quotes_count
            )

        # Parse quotes
        for quote_elem in await html.select(".quote"):
            text_elem = quote_elem.select_first(".text")
            author_elem = quote_elem.select_first(".author")

            if text_elem and author_elem:
                quote_text = text_elem.text.strip()
                author = author_elem.text.strip()

                # Get tags
                tags = [tag.text for tag in quote_elem.select(".tag")]

                self.quotes_count += 1

                if self.logger:
                    self.logger.debug(
                        "Scraped quote",
                        quote_number=self.quotes_count,
                        author=author,
                        tags_count=len(tags),
                        quote_length=len(quote_text),
                    )

                yield {
                    "text": quote_text,
                    "author": author,
                    "tags": tags,
                }

        # Follow next page link (limit to 3 pages for demo)
        if self.quotes_count < 30:
            next_link = await html.select_first("li.next > a")
            if next_link:
                next_url = next_link.attr("href")
                if next_url and self.logger:
                    self.logger.info(
                        "Following next page",
                        next_url=next_url,
                        quotes_collected=self.quotes_count,
                    )
                    yield html.follow(next_url, callback=self.parse)
        else:
            if self.logger:
                self.logger.info(
                    "Reached quote limit, stopping", total_quotes=self.quotes_count
                )


def demo_1_configure_before_spider():
    """Demo 1: Configure hybrid logger globally before creating spider."""
    print("\n" + "=" * 70)
    print("Demo 1: Global hybrid logger configuration")
    print("=" * 70)

    # Configure hybrid logger once at startup
    configure_hybrid_logger(json_log_file="data/demo1_logs.jsonl", log_level="INFO")

    # Create spider - it will use the configured global logger
    _ = HybridLoggerSpider(logger={"component": "Demo1Spider"})

    # The logger will:
    # - Show colored text in console
    # - Write JSON to data/demo1_logs.jsonl

    print("Spider created with hybrid logger")
    print("Check data/demo1_logs.jsonl for JSON logs after running\n")


def demo_2_custom_logger_instance():
    """Demo 2: Create custom logger instance with specific context."""
    print("\n" + "=" * 70)
    print("Demo 2: Custom logger instance with context")
    print("=" * 70)

    configure_hybrid_logger(
        json_log_file="data/demo2_logs.jsonl",
        log_level="DEBUG",  # More verbose
    )

    # Create logger with custom context that will appear in all logs
    custom_logger = get_logger(
        component="QuotesSpider", version="2.0", environment="development"
    )

    # Pass the configured logger to spider
    _ = HybridLoggerSpider(logger=custom_logger)

    print("Spider created with custom logger context")
    print("All logs will include: component, version, environment fields\n")


def demo_3_run_spider_with_hybrid_logger():
    """Demo 3: Run a full spider with hybrid logging."""
    print("\n" + "=" * 70)
    print("Demo 3: Running spider with hybrid logger (live demo)")
    print("=" * 70)

    # Configure hybrid logger
    json_log_file = "data/hybrid_spider_logs.jsonl"
    configure_hybrid_logger(json_log_file=json_log_file, log_level="INFO")

    print("Starting spider crawl...")
    print("Watch the console for colored text logs")
    print(f"JSON logs will be written to: {json_log_file}\n")

    try:
        run_spider(
            HybridLoggerSpider,
            logger={"component": "QuotesSpider", "mode": "hybrid"},
            concurrency=8,
            request_timeout=10,
        )

        print("\n" + "=" * 70)
        print("Crawl completed! Checking JSON log file...")
        print("=" * 70)

        # Show sample of JSON logs
        if os.path.exists(json_log_file):
            import json

            with open(json_log_file, "r") as f:
                lines = f.readlines()
                print(f"\nTotal JSON log entries: {len(lines)}")
                print("\nFirst 3 log entries (formatted):")
                for i, line in enumerate(lines[:3], 1):
                    try:
                        log_entry = json.loads(line)
                        print(
                            f"\n{i}. {log_entry.get('level', 'N/A')} - {log_entry.get('message', 'N/A')}"
                        )
                        print(f"   Fields: {', '.join(log_entry.keys())}")
                    except json.JSONDecodeError:
                        print(f"{i}. Could not parse: {line[:50]}...")
        else:
            print(f"No JSON log file found at {json_log_file}")

    except KeyboardInterrupt:
        print("\n\nCrawl interrupted by user")
    except Exception as e:
        print(f"\n\nError during crawl: {e}")


def show_json_log_analysis():
    """Show how to analyze JSON logs programmatically."""
    print("\n" + "=" * 70)
    print("Bonus: Analyzing JSON logs programmatically")
    print("=" * 70)

    json_file = "data/hybrid_spider_logs.jsonl"
    if not os.path.exists(json_file):
        print(f"No log file at {json_file} - run demo_3 first")
        return

    import json

    # Parse all logs
    logs = []
    with open(json_file, "r") as f:
        for line in f:
            try:
                logs.append(json.loads(line))
            except json.JSONDecodeError:
                pass

    if not logs:
        print("No logs to analyze")
        return

    # Analyze logs
    print(f"\nTotal logs: {len(logs)}")

    # Group by level
    by_level = {}
    for log in logs:
        level = log.get("level", "UNKNOWN")
        by_level[level] = by_level.get(level, 0) + 1

    print("\nLogs by level:")
    for level, count in sorted(by_level.items()):
        print(f"  {level}: {count}")

    # Find specific patterns
    info_logs = [log for log in logs if log.get("level") == "INFO"]
    if info_logs:
        print(f"\nSample INFO message: {info_logs[0].get('message', 'N/A')}")

    # Show unique components
    components = set(log.get("component") for log in logs if "component" in log)
    if components:
        print(f"\nComponents logging: {', '.join(components)}")


if __name__ == "__main__":
    import sys

    print("=" * 70)
    print("Silkworm Hybrid Logger Demo")
    print("=" * 70)
    print()
    print("This demo shows how to configure a logger that:")
    print("  • Outputs human-readable text to console (with colors)")
    print("  • Writes structured JSON to file (for analysis)")
    print()

    if len(sys.argv) > 1 and sys.argv[1] == "--run":
        # Actually run the spider
        demo_3_run_spider_with_hybrid_logger()
        show_json_log_analysis()
    else:
        # Just show configuration examples
        demo_1_configure_before_spider()
        demo_2_custom_logger_instance()

        print("\n" + "=" * 70)
        print("To actually run the spider and see hybrid logging in action:")
        print("  python examples/hybrid_logger_demo.py --run")
        print("=" * 70)
