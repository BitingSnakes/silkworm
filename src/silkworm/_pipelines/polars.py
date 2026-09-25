from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

try:
    import polars as pl  # type: ignore[import-not-found]

    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False

from ..logging import get_logger
from .base import _log_pipeline_item

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class PolarsPipeline:
    """
    Pipeline that writes items to a Parquet file using Polars.

    Parquet is a columnar storage format optimized for analytics workloads.
    This pipeline uses Polars for fast and efficient Parquet serialization.

    Example:
        from silkworm.pipelines import PolarsPipeline

        pipeline = PolarsPipeline("data/items.parquet")
        # Or append to existing file:
        pipeline = PolarsPipeline("data/items.parquet", mode="append")

    Reading Parquet files:
        import polars as pl

        # Read entire dataset
        df = pl.read_parquet("data/items.parquet")

        # Or read with filters/projections (memory efficient)
        df = pl.scan_parquet("data/items.parquet").filter(
            pl.col("author") == "John"
        ).collect()
    """

    def __init__(
        self,
        path: str | Path = "items.parquet",
        *,
        mode: str = "write",
    ) -> None:
        """
        Initialize PolarsPipeline.

        Args:
            path: Path to the output file (default: "items.parquet")
            mode: Write mode - "write" (overwrite) or "append" (default: "write")
        """
        if not POLARS_AVAILABLE:
            raise ImportError(
                "polars is required for PolarsPipeline. Install it with: pip install silkworm-rs[polars]",
            )
        if mode not in ("write", "append"):
            raise ValueError(f"mode must be 'write' or 'append', got '{mode}'")

        self.path = Path(path)
        self.mode = mode
        self._items: list[JSONValue] = []
        self.logger = get_logger(component="PolarsPipeline")

    async def open(self, spider: Spider) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._items = []
        self.logger.info("Opened Polars pipeline", path=str(self.path), mode=self.mode)

    async def close(self, spider: Spider) -> None:
        if self._items:
            df = pl.DataFrame(self._items)
            if self.mode == "append" and self.path.exists():
                # Read existing data and concatenate
                existing_df = pl.read_parquet(self.path)
                df = pl.concat([existing_df, df])
            df.write_parquet(self.path)
        self.logger.info("Closed Polars pipeline", path=str(self.path))

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        self._items.append(item)
        _log_pipeline_item(
            self,
            "Buffered item for Parquet",
            path=str(self.path),
            spider=spider.name,
        )
        return item
