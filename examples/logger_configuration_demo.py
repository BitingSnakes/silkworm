"""
Example demonstrating configurable logger in Spider initialization.

This example shows how to configure a logger when creating a spider instance,
rather than manually calling get_logger() in the __init__ method.
"""

from silkworm import Spider, Response
from silkworm.logging import get_logger


class LoggingSpider(Spider):
    """Spider that uses the logger configured during initialization."""

    name = "logging_demo"
    start_urls = ("https://quotes.toscrape.com/",)

    async def parse(self, response: Response):
        # The logger is already configured and available as self.logger
        if self.logger:
            self.logger.info("Parsing page", url=response.url)

        # Just a simple example - no actual scraping
        yield {"url": response.url, "status": response.status}


class ManualLoggerSpider(Spider):
    """
    Alternative: Spider that manually creates its logger (old way).
    This still works for backward compatibility.
    """

    name = "manual_logger_demo"
    start_urls = ("https://quotes.toscrape.com/",)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Manually create logger if needed
        if self.logger is None:
            self.logger = get_logger(component="ManualLogger", spider=self.name)

    async def parse(self, response: Response):
        if self.logger:
            self.logger.info("Parsing page", url=response.url)
        yield {"url": response.url, "status": response.status}


def demo_1_logger_from_dict():
    """Demo 1: Configure logger from a dict context."""
    print("\n=== Demo 1: Logger from dict ===")
    spider = LoggingSpider(logger={"component": "QuotesSpider", "version": "1.0"})
    print(f"Spider logger configured: {spider.logger is not None}")


def demo_2_logger_instance():
    """Demo 2: Pass a pre-configured logger instance."""
    print("\n=== Demo 2: Logger instance ===")
    custom_logger = get_logger(component="CustomComponent", env="production")
    spider = LoggingSpider(logger=custom_logger)
    print(f"Spider using custom logger: {spider.logger is custom_logger}")


def demo_3_no_logger():
    """Demo 3: Spider without logger (logger is None)."""
    print("\n=== Demo 3: No logger ===")
    spider = LoggingSpider()
    print(f"Spider logger is None: {spider.logger is None}")


def demo_4_via_run_spider():
    """Demo 4: Configuring logger when using run_spider."""
    print("\n=== Demo 4: Via run_spider ===")
    print("You can pass logger configuration through spider_kwargs in run_spider:")
    print("run_spider(LoggingSpider, logger={'component': 'MySpider'})")


if __name__ == "__main__":
    print("Silkworm Spider Logger Configuration Examples")
    print("=" * 50)

    demo_1_logger_from_dict()
    demo_2_logger_instance()
    demo_3_no_logger()
    demo_4_via_run_spider()

    print("\n" + "=" * 50)
    print("All examples completed successfully!")
