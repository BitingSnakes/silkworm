from __future__ import annotations

import csv
import io
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING

from ..logging import Logger, get_logger
from .base import log_pipeline_item

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class CSVPipeline:
    """Stream flattened items to a UTF-8 CSV file.

    Args:
        path: Destination file path.
        fieldnames: Fixed column order. When omitted, columns are inferred from
            the first item.

    Nested mappings use underscore-separated keys and list values are joined
    with commas.
    """

    def __init__(
        self,
        path: str | Path = "items.csv",
        *,
        fieldnames: list[str] | None = None,
    ) -> None:
        self.path: Path = Path(path)
        self.fieldnames = fieldnames
        self._fp: io.TextIOWrapper | None = None
        self._writer: csv.DictWriter[str] | None = None
        self._header_written = False
        self.logger: Logger = get_logger(component="CSVPipeline")

    async def open(self, spider: Spider) -> None:
        """Create parent directories and open a fresh CSV destination."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fp = self.path.open("w", encoding="utf-8", newline="")
        self._header_written = False
        self.logger.info("Opened CSV pipeline", path=str(self.path))

    async def close(self, spider: Spider) -> None:
        """Flush and close the CSV file if it is open."""
        fp = self._fp
        self._fp = None
        self._writer = None
        if fp:
            fp.close()
            self.logger.info("Closed CSV pipeline", path=str(self.path))

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        """Flatten and append one mapping item to the CSV file."""
        if not self._fp:
            raise RuntimeError("CSVPipeline not opened")

        # Flatten nested structures if item is a dict
        if isinstance(item, Mapping):
            flat_item = self._flatten_dict(item)
        else:
            flat_item = {"value": str(item)}

        # Initialize writer with fieldnames from first item if not provided
        if not self._writer:
            if self.fieldnames is None:
                self.fieldnames = list(flat_item.keys())
            self._writer = csv.DictWriter(
                self._fp,
                fieldnames=self.fieldnames,
                extrasaction="ignore",
            )

        # Write header if first item
        if not self._header_written:
            self._writer.writeheader()
            self._header_written = True

        self._writer.writerow(flat_item)
        self._fp.flush()
        log_pipeline_item(
            self, "Wrote item to CSV", path=str(self.path), spider=spider.name
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
