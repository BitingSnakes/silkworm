from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING

try:
    import openpyxl  # type: ignore[import-not-found, import-untyped]

    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False

from ..logging import get_logger
from .base import _log_pipeline_item

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class ExcelPipeline:
    """
    Pipeline that writes items to an Excel file (.xlsx).

    Example:
        from silkworm.pipelines import ExcelPipeline

        pipeline = ExcelPipeline("data/items.xlsx", sheet_name="quotes")
    """

    def __init__(
        self,
        path: str | Path = "items.xlsx",
        *,
        sheet_name: str = "Sheet1",
    ) -> None:
        """
        Initialize ExcelPipeline.

        Args:
            path: Path to the output file (default: "items.xlsx")
            sheet_name: Name of the Excel sheet (default: "Sheet1")
        """
        if not OPENPYXL_AVAILABLE:
            raise ImportError(
                "openpyxl is required for ExcelPipeline. Install it with: pip install silkworm-rs[excel]",
            )

        self.path = Path(path)
        self.sheet_name = sheet_name
        self._items: list[JSONValue] = []
        self.logger = get_logger(component="ExcelPipeline")

    async def open(self, spider: Spider) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._items = []
        self.logger.info("Opened Excel pipeline", path=str(self.path))

    async def close(self, spider: Spider) -> None:
        if self._items:
            wb = openpyxl.Workbook()
            ws = wb.active
            ws.title = self.sheet_name

            # Get fieldnames from first item
            if isinstance(self._items[0], Mapping):
                flat_items: list[dict[str, JSONValue | str]] = []
                for item in self._items:
                    if isinstance(item, Mapping):
                        flat_items.append(self._flatten_dict(item))
                    else:
                        flat_items.append({"value": str(item)})
                fieldnames = list(flat_items[0].keys())

                # Write header
                ws.append(fieldnames)

                # Write data
                for item in flat_items:
                    ws.append([item.get(field) for field in fieldnames])
            else:
                # Simple values
                ws.append(["value"])
                for item in self._items:
                    ws.append([str(item)])

            wb.save(self.path)
        self.logger.info("Closed Excel pipeline", path=str(self.path))

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        self._items.append(item)
        _log_pipeline_item(
            self,
            "Buffered item for Excel",
            path=str(self.path),
            spider=spider.name,
        )
        return item

    def _flatten_dict(
        self,
        data: Mapping[str, JSONValue],
        parent_key: str = "",
        sep: str = "_",
    ) -> dict[str, JSONValue | str]:
        """Flatten a nested dictionary structure."""
        items: list[tuple[str, JSONValue | str]] = []
        for key, value in data.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            if isinstance(value, Mapping):
                items.extend(self._flatten_dict(value, new_key, sep=sep).items())
            elif isinstance(value, list):
                # Convert list to comma-separated string
                items.append((new_key, ", ".join(str(v) for v in value)))
            else:
                items.append((new_key, value))
        return dict(items)
