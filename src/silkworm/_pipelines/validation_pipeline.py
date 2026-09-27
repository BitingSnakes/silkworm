from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Literal, Protocol, cast, runtime_checkable

from ..exceptions import DropItem
from ..logging import Logger, get_logger

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


@runtime_checkable
class ModelSchema(Protocol):
    """A Pydantic-style model class: ``model_validate`` returns a model instance."""

    def model_validate(self, obj: object) -> object:
        """Validate ``obj`` and return a model instance, raising on failure."""
        ...


type ItemValidator = Callable[[JSONValue], JSONValue]
type ItemSchema = ModelSchema | type[object] | ItemValidator


class ValidationPipeline:
    """Validate items against a schema and drop (or reject) invalid ones.

    ``schema`` is either a Pydantic-style model class (anything exposing
    ``model_validate``, such as ``pydantic.BaseModel`` subclasses) or a callable
    that returns the validated item and raises ``ValueError``/``TypeError``
    (Pydantic's ``ValidationError`` is a ``ValueError``) for invalid input.
    Model results are converted back to JSON-compatible dicts with
    ``model_dump(mode="json")``, so later pipelines receive normalized data.

    Invalid items raise :class:`~silkworm.exceptions.DropItem` with reason
    ``"invalid"`` (counted under ``items_dropped``), or propagate the validation
    error with ``on_invalid="raise"``. Combine with the engine's
    ``max_item_drop_rate`` or ``min_items`` to fail crawls when a site redesign
    breaks extraction. The first ``log_limit`` failures are logged with details.

    Args:
        schema: Model class or validator callable.
        on_invalid: ``"drop"`` to discard invalid items, ``"raise"`` to fail the
            emitting callback.
        log_limit: Invalid items logged at warning level before going quiet.

    Attributes:
        valid: Number of items that passed validation.
        invalid: Number of items that failed validation.
    """

    def __init__(
        self,
        schema: ItemSchema,
        *,
        on_invalid: Literal["drop", "raise"] = "drop",
        log_limit: int = 10,
    ) -> None:
        if on_invalid not in {"drop", "raise"}:
            msg = "on_invalid must be 'drop' or 'raise'"
            raise ValueError(msg)
        if log_limit < 0:
            msg = "log_limit must be non-negative"
            raise ValueError(msg)
        if not isinstance(schema, ModelSchema) and not callable(schema):
            msg = "schema must be a model class with model_validate or a callable"
            raise TypeError(msg)
        self.schema: ItemSchema = schema
        self.on_invalid: Literal["drop", "raise"] = on_invalid
        self.log_limit: int = log_limit
        self.valid: int = 0
        self.invalid: int = 0
        self.logger: Logger = get_logger(component="ValidationPipeline")

    async def open(self, spider: Spider) -> None:
        """Reset the validation counters."""
        self.valid = 0
        self.invalid = 0

    async def close(self, spider: Spider) -> None:
        """Log how many items passed and failed validation."""
        self.logger.info(
            "Item validation summary",
            spider=spider.name,
            schema=self._schema_name(),
            valid=self.valid,
            invalid=self.invalid,
        )

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        """Return the validated item, or drop/raise when it is invalid."""
        try:
            validated = self._validate(item)
        except (ValueError, TypeError) as exc:
            self.invalid += 1
            if self.invalid <= self.log_limit:
                self.logger.warning(
                    "Invalid item",
                    spider=spider.name,
                    schema=self._schema_name(),
                    error=str(exc),
                    item=str(item)[:500],
                )
                if self.invalid == self.log_limit:
                    self.logger.warning(
                        "Suppressing further invalid item logs",
                        spider=spider.name,
                        log_limit=self.log_limit,
                    )
            if self.on_invalid == "raise":
                raise
            raise DropItem(
                f"Item failed {self._schema_name()} validation: {exc}",
                reason="invalid",
            ) from exc
        self.valid += 1
        return validated

    def _validate(self, item: JSONValue) -> JSONValue:
        if isinstance(self.schema, ModelSchema):
            model = self.schema.model_validate(item)
            dump = getattr(model, "model_dump", None)
            if callable(dump):
                return cast("JSONValue", dump(mode="json"))
            return cast("JSONValue", model)
        return cast("ItemValidator", self.schema)(item)

    def _schema_name(self) -> str:
        return getattr(self.schema, "__name__", type(self.schema).__name__)
