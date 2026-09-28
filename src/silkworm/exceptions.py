from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._stats import CrawlResult


class SilkwormError(Exception):
    """Base exception for the framework."""


class HttpError(SilkwormError):
    """Raised when an HTTP request fails."""


class HttpTimeoutError(HttpError):
    """Raised when an HTTP request exceeds its timeout; usually transient."""


class HttpConnectionError(HttpError):
    """Raised when the transport fails (DNS, connect, reset); usually transient."""


class ResponseTooLargeError(HttpError):
    """Raised when a response body exceeds the configured size limit."""


class SpiderError(SilkwormError):
    """Raised when a spider callback errors."""


class BatchPipelineError(SilkwormError):
    """Raised when a backend rejects one or more items from a bulk request."""

    def __init__(self, pipeline: str, *, total: int, failed: int) -> None:
        super().__init__(
            f"{pipeline} rejected {failed} of {total} items during batch processing"
        )
        self.pipeline = pipeline
        self.total = total
        self.failed = failed


class SelectorError(SilkwormError):
    """Raised when CSS/XPath selector evaluation fails."""


class MarkdownConversionError(SilkwormError):
    """Raised when HTML to Markdown conversion fails."""


class IgnoreRequest(SilkwormError):
    """Raise from a request or response middleware to drop a request silently.

    Ignored requests are counted under ``ignored_requests`` (labelled by
    ``reason``) and are neither errors nor passed to errbacks.

    Args:
        message: Human-readable explanation used in debug logs.
        reason: Short, low-cardinality label used in statistics.
    """

    def __init__(self, message: str = "", *, reason: str = "ignored") -> None:
        super().__init__(message or reason)
        self.reason = reason


class DropItem(SilkwormError):
    """Raise from an item pipeline to discard an item.

    Later pipelines are skipped, the item is counted under ``items_dropped``
    (labelled by ``reason``), and ``emit()`` returns normally.

    Args:
        message: Human-readable explanation used in logs.
        reason: Short, low-cardinality label used in statistics.
    """

    def __init__(self, message: str = "", *, reason: str = "dropped") -> None:
        super().__init__(message or reason)
        self.reason = reason


class CloseSpider(SilkwormError):
    """Raise from a callback, middleware, or pipeline to stop the crawl gracefully.

    Pending requests are discarded (or kept for resuming when a job directory
    is configured), in-flight requests finish, and the crawl result reports
    ``reason`` as its close reason.
    """

    def __init__(self, reason: str = "close_spider") -> None:
        super().__init__(reason)
        self.reason = reason


class CrawlFailedError(SilkwormError):
    """Raised by ``Engine.run()`` when the crawl violates its failure policy.

    Attributes:
        result: The complete :class:`~silkworm.CrawlResult`, including the
            policy violations in ``result.failures``.
    """

    def __init__(self, result: CrawlResult) -> None:
        super().__init__(
            f"Crawl of {result.spider!r} failed: " + "; ".join(result.failures)
        )
        self.result = result
