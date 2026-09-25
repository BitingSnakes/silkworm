from __future__ import annotations

import io
import json
from pathlib import Path
from typing import TYPE_CHECKING

try:
    import opendal  # type: ignore[import-not-found]

    OPENDAL_AVAILABLE = True
except ImportError:
    OPENDAL_AVAILABLE = False

from ..logging import LogLevel, get_logger
from .base import _log_pipeline_item

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class JsonLinesPipeline:
    def __init__(
        self,
        path: str | Path = "items.jl",
        *,
        use_opendal: bool | None = None,
        log_level: LogLevel = "DEBUG",
    ) -> None:
        self.path = Path(path)
        self.log_level = log_level
        self._fp: io.TextIOWrapper | None = None
        self._operator: opendal.AsyncOperator | None = None
        self._object_path: str | None = None
        self._use_opendal = OPENDAL_AVAILABLE if use_opendal is None else use_opendal
        if self._use_opendal and not OPENDAL_AVAILABLE:
            raise ImportError(
                "opendal is required for async JsonLinesPipeline writes. "
                "Install it with: pip install silkworm-rs[s3]",
            )
        self.logger = get_logger(component="JsonLinesPipeline")

    async def open(self, spider: Spider) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self._use_opendal:
            self._operator = opendal.AsyncOperator("fs", root=str(self.path.parent))
            self._object_path = self.path.name
            self.logger.info(
                "Opened JSONL pipeline",
                path=str(self.path),
                backend="opendal",
            )
        else:
            self._fp = self.path.open("a", encoding="utf-8")
            self.logger.info("Opened JSONL pipeline", path=str(self.path))

    async def close(self, spider: Spider) -> None:
        if self._operator:
            self._operator = None
            self._object_path = None
            self.logger.info(
                "Closed JSONL pipeline",
                path=str(self.path),
                backend="opendal",
            )
        if self._fp:
            self._fp.close()
            self._fp = None
            self.logger.info("Closed JSONL pipeline", path=str(self.path))

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        line = json.dumps(item, ensure_ascii=False)
        if self._operator:
            try:
                await self._append_with_opendal(line + "\n")
            except Exception as exc:
                self.logger.warning(
                    "OpenDAL write failed, falling back to local file handle",
                    path=str(self.path),
                    error=str(exc),
                )
                self._operator = None
                self._object_path = None
                self._fp = self.path.open("a", encoding="utf-8")
            else:
                _log_pipeline_item(
                    self,
                    "Wrote item to JSONL",
                    path=str(self.path),
                    spider=spider.name,
                    backend="opendal",
                )
                return item

        if not self._fp:
            raise RuntimeError("JsonLinesPipeline not opened")
        self._fp.write(line + "\n")
        self._fp.flush()
        _log_pipeline_item(
            self,
            "Wrote item to JSONL",
            path=str(self.path),
            spider=spider.name,
        )
        return item

    async def _append_with_opendal(self, data: str) -> None:
        if not self._operator or not self._object_path:
            raise RuntimeError("JsonLinesPipeline not opened")

        if not self._operator.capability().write_can_append:
            raise RuntimeError("OpenDAL operator does not support append writes")

        await self._operator.write(self._object_path, data.encode("utf-8"), append=True)
