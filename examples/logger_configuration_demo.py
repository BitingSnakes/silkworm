"""
Three ways to give a spider a logger.

Every spider has `self.log`, which always works. You can also choose what the
logger looks like when you create the spider, with the `logger=` argument:

1. No argument          -> a default logger is created the first time you use `self.log`.
2. `logger={...}` dict  -> a logger is created with these extra fields on every line.
3. `logger=my_logger`   -> use a logger you already created yourself.

This script only creates spiders and prints what happened; it doesn't crawl.

How to run:
    python examples/logger_configuration_demo.py
"""

from __future__ import annotations

from silkworm import Response, Spider
from silkworm.logging import get_logger


class LoggingSpider(Spider):
    name = "logging_demo"
    start_urls = ("https://quotes.toscrape.com/",)

    async def parse(self, response: Response):
        # `self.log` is always available, whichever way the logger was set up.
        self.log.info("Parsing page", url=response.url)
        yield {"url": response.url, "status": response.status}


def demo_default_logger() -> None:
    print("\n=== 1. No logger argument ===")
    spider = LoggingSpider()
    # Nothing is set yet; `self.log` creates a logger the first time it's used.
    print(f"spider.logger before using self.log: {spider.logger}")
    spider.log.info("Hello from the default logger")
    print(f"spider.logger after using self.log:  {spider.logger is not None}")


def demo_logger_from_dict() -> None:
    print("\n=== 2. Logger from a dict ===")
    # Every log line from this spider will include component=... and version=...
    spider = LoggingSpider(logger={"component": "QuotesSpider", "version": "1.0"})
    spider.log.info("Hello from a dict-configured logger")


def demo_logger_instance() -> None:
    print("\n=== 3. Your own logger instance ===")
    my_logger = get_logger(component="CustomComponent", env="production")
    spider = LoggingSpider(logger=my_logger)
    print(f"Spider uses my_logger: {spider.logger is my_logger}")
    spider.log.info("Hello from my own logger")


if __name__ == "__main__":
    demo_default_logger()
    demo_logger_from_dict()
    demo_logger_instance()

    print("\nTo crawl with a configured logger, pass a spider instance to run_spider:")
    print("    run_spider(LoggingSpider(logger={'component': 'MySpider'}))")
