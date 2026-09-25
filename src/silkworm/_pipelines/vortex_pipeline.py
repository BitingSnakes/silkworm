from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

try:
    import vortex  # type: ignore[import-not-found]
    import vortex.io  # type: ignore[import-not-found]

    VORTEX_AVAILABLE = True
except ImportError:
    VORTEX_AVAILABLE = False

from ..logging import get_logger
from .base import _log_pipeline_item

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class VortexPipeline:
    """
    Pipeline that writes items to a Vortex file using the vortex-data library.

    Vortex is a next-generation columnar file format optimized for high-performance
    data processing with 100x faster random access reads compared to Parquet, 10-20x
    faster scans, and similar compression ratios. It provides zero-copy compatibility
    with Apache Arrow.

    Example:
        from silkworm.pipelines import VortexPipeline

        pipeline = VortexPipeline("data/items.vortex")

    Reading Vortex files:
        import vortex

        # Open and read a Vortex file
        vortex_file = vortex.file.open("data/items.vortex")
        arrow_table = vortex_file.to_arrow().read_all()

        # Or convert to other formats
        df = vortex_file.to_polars()  # Polars DataFrame
        df = vortex_file.to_dataset().to_table()  # PyArrow Table
    """

    def __init__(
        self,
        path: str | Path = "items.vortex",
    ) -> None:
        """
        Initialize VortexPipeline.

        Args:
            path: Path to the output file (default: "items.vortex")
        """
        if not VORTEX_AVAILABLE:
            raise ImportError(
                "vortex is required for VortexPipeline. Install it with: pip install silkworm-rs[vortex]",
            )

        self.path = Path(path)
        self._items: list[JSONValue] = []
        self.logger = get_logger(component="VortexPipeline")

    async def open(self, spider: Spider) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._items = []
        self.logger.info("Opened Vortex pipeline", path=str(self.path))

    async def close(self, spider: Spider) -> None:
        if self._items:
            # Convert items list to PyArrow Table
            # Vortex can directly accept PyArrow tables for efficient writing
            import pyarrow as pa  # type: ignore[import-not-found, import-untyped]

            table = pa.Table.from_pylist(self._items)

            # Write the table to a Vortex file
            vortex.io.write(table, str(self.path))  # pyright: ignore[reportPossiblyUnboundVariable]

            self.logger.info(
                "Closed Vortex pipeline",
                path=str(self.path),
                items_written=len(self._items),
            )
        else:
            self.logger.info("Closed Vortex pipeline (no items)", path=str(self.path))

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        self._items.append(item)
        _log_pipeline_item(
            self,
            "Buffered item for Vortex",
            path=str(self.path),
            spider=spider.name,
        )
        return item
