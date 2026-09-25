from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

try:
    import yaml  # type: ignore[import-untyped]

    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

from ..logging import get_logger
from .base import log_pipeline_item

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class YAMLPipeline:
    """
    Pipeline that writes items to a YAML file.

    Example:
        from silkworm.pipelines import YAMLPipeline

        pipeline = YAMLPipeline("data/items.yaml")
    """

    def __init__(
        self,
        path: str | Path = "items.yaml",
    ) -> None:
        """
        Initialize YAMLPipeline.

        Args:
            path: Path to the output file (default: "items.yaml")
        """
        if not YAML_AVAILABLE:
            raise ImportError(
                "pyyaml is required for YAMLPipeline. Install it with: pip install silkworm-rs[yaml]",
            )

        self.path = Path(path)
        self._items: list[JSONValue] = []
        self.logger = get_logger(component="YAMLPipeline")

    async def open(self, spider: Spider) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._items = []
        self.logger.info("Opened YAML pipeline", path=str(self.path))

    async def close(self, spider: Spider) -> None:
        if self._items:
            with self.path.open("w", encoding="utf-8") as f:
                yaml.dump(self._items, f, default_flow_style=False, allow_unicode=True)  # pyright: ignore[reportPossiblyUnboundVariable]
        self.logger.info("Closed YAML pipeline", path=str(self.path))

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        self._items.append(item)
        log_pipeline_item(
            self,
            "Buffered item for YAML",
            path=str(self.path),
            spider=spider.name,
        )
        return item
