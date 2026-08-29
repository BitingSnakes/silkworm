from __future__ import annotations

from ..exceptions import SilkwormError


class DeclarativeError(SilkwormError):
    """Base exception for declarative extraction."""


class DeclarativeConfigurationError(DeclarativeError):
    """Raised when an item declaration cannot be compiled."""


class FieldExtractionError(DeclarativeError):
    """Raised when a declared field cannot be extracted."""


class MissingFieldError(FieldExtractionError):
    """Raised when a required element or attribute is absent."""


class FieldTransformError(FieldExtractionError):
    """Raised when a field transform fails or returns an invalid value."""


class DeclarativeSerializationError(DeclarativeError):
    """Raised when an item cannot be represented as a JSON value."""


__all__ = [
    "DeclarativeConfigurationError",
    "DeclarativeError",
    "DeclarativeSerializationError",
    "FieldExtractionError",
    "FieldTransformError",
    "MissingFieldError",
]
