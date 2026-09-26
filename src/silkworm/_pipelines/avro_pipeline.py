from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

try:
    import fastavro  # type: ignore[import-not-found]

    FASTAVRO_AVAILABLE = True
except ImportError:
    FASTAVRO_AVAILABLE = False

from ..logging import Logger, get_logger
from .base import log_pipeline_item

if TYPE_CHECKING:
    from collections.abc import Mapping

    from .._types import JSONLike, JSONValue
    from ..spiders import Spider


class AvroPipeline:
    """
    Pipeline that writes items to an Avro file.

    Avro is a row-oriented data serialization system with compact binary format.
    Items are buffered until the pipeline closes.

    Args:
        path: Output Avro file.
        schema: Explicit Avro schema. When omitted, a simple record schema is
            inferred from the first item.

    Example::

        from silkworm.pipelines import AvroPipeline

        schema = {
            "type": "record",
            "name": "Quote",
            "fields": [
                {"name": "text", "type": "string"},
                {"name": "author", "type": "string"},
                {"name": "tags", "type": {"type": "array", "items": "string"}},
            ],
        }
        pipeline = AvroPipeline("data/items.avro", schema=schema)
    """

    def __init__(
        self,
        path: str | Path = "items.avro",
        *,
        schema: Mapping[str, JSONLike] | None = None,
    ) -> None:
        """
        Initialize AvroPipeline.

        Args:
            path: Path to the output file (default: "items.avro")
            schema: Avro schema dict. If None, will infer from first item.
        """
        if not FASTAVRO_AVAILABLE:
            raise ImportError(
                "fastavro is required for AvroPipeline. Install it with: pip install silkworm-rs[avro]",
            )

        self.path: Path = Path(path)
        self.schema: Mapping[str, JSONLike] | None = schema
        self._items: list[JSONValue] = []
        self.logger: Logger = get_logger(component="AvroPipeline")

    async def open(self, spider: Spider) -> None:
        """Create parent directories and reset the in-memory item buffer."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._items = []
        self.logger.info("Opened Avro pipeline", path=str(self.path))

    async def close(self, spider: Spider) -> None:
        """Infer or apply the schema and write all buffered items."""
        if self._items:
            if self.schema is None:
                # Infer schema from first item
                schema = self._infer_schema(self._items[0])
            else:
                # fastavro expects a plain dict.
                schema = dict(self.schema)

            with self.path.open("wb") as f:
                fastavro.writer(f, schema, self._items)  # pyright: ignore[reportPossiblyUnboundVariable]
        self.logger.info("Closed Avro pipeline", path=str(self.path))

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        """Buffer one item for serialization during :meth:`close`."""
        self._items.append(item)
        log_pipeline_item(
            self,
            "Buffered item for Avro",
            path=str(self.path),
            spider=spider.name,
        )
        return item

    def _infer_schema(self, item: JSONValue) -> dict[str, Any]:
        """Infer a simple Avro schema from the first item."""
        fields = []
        if isinstance(item, dict):
            for key, value in item.items():
                field_type = self._infer_type(value)
                fields.append({"name": key, "type": ["null", field_type]})

        return {
            "type": "record",
            "name": "ScrapedItem",
            "fields": fields,
        }

    def _infer_type(self, value: JSONValue) -> str | dict[str, Any]:
        """Infer Avro type from Python value."""
        if isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            return "long"
        elif isinstance(value, float):
            return "double"
        elif isinstance(value, str):
            return "string"
        elif isinstance(value, list):
            if value:
                item_type = self._infer_type(value[0])
                return {"type": "array", "items": item_type}
            return {"type": "array", "items": "string"}
        elif isinstance(value, dict):
            # For nested dicts, convert to JSON string
            return "string"
        else:
            return "string"
