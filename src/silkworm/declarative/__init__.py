"""Declarative item extraction from :class:`~silkworm.HTMLResponse` objects.

Subclass :class:`Item` and assign :class:`Text` or :class:`Attr` descriptors to
compile a reusable extraction plan with validation, transforms, and nested
items.
"""

from __future__ import annotations

from .exceptions import (
    DeclarativeConfigurationError,
    DeclarativeError,
    DeclarativeSerializationError,
    FieldExtractionError,
    FieldTransformError,
    MissingFieldError,
)
from .fields import Attr, Field, Text
from .items import Item
from .plans import ExtractionPlan, FieldPlan

__all__ = [
    "Attr",
    "DeclarativeConfigurationError",
    "DeclarativeError",
    "DeclarativeSerializationError",
    "ExtractionPlan",
    "Field",
    "FieldExtractionError",
    "FieldPlan",
    "FieldTransformError",
    "Item",
    "MissingFieldError",
    "Text",
]
