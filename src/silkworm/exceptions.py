from __future__ import annotations


class SilkwormError(Exception):
    """Base exception for the framework."""


class HttpError(SilkwormError):
    """Raised when an HTTP request fails."""


class SpiderError(SilkwormError):
    """Raised when a spider callback errors."""


class SelectorError(SilkwormError):
    """Raised when CSS/XPath selector evaluation fails."""
