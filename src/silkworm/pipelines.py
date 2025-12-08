from __future__ import annotations
import io
import json
import sqlite3
from pathlib import Path
from typing import Any, Protocol

if True:
    from .spiders import Spider  # type: ignore
from .logging import get_logger


class ItemPipeline(Protocol):
    async def open(self, spider: "Spider") -> None: ...
    async def close(self, spider: "Spider") -> None: ...
    async def process_item(self, item: Any, spider: "Spider") -> Any: ...


class JsonLinesPipeline:
    def __init__(
        self, path: str | Path = "items.jl", *, ensure_ascii: bool = False
    ) -> None:
        self.path = Path(path)
        self.ensure_ascii = ensure_ascii
        self._fp: io.TextIOWrapper | None = None  # type: ignore[name-defined]
        self.logger = get_logger(component="JsonLinesPipeline")

    async def open(self, spider: "Spider") -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fp = self.path.open("a", encoding="utf-8")
        self.logger.info("Opened JSONL pipeline", path=str(self.path))

    async def close(self, spider: "Spider") -> None:
        if self._fp:
            self._fp.close()
            self._fp = None
            self.logger.info("Closed JSONL pipeline", path=str(self.path))

    async def process_item(self, item: Any, spider: "Spider") -> Any:
        if not self._fp:
            raise RuntimeError("JsonLinesPipeline not opened")
        line = json.dumps(item, ensure_ascii=self.ensure_ascii)
        self._fp.write(line + "\n")
        self._fp.flush()
        self.logger.debug(
            "Wrote item to JSONL", path=str(self.path), spider=spider.name
        )
        return item


class SQLitePipeline:
    def __init__(self, path: str | Path = "items.db", table: str = "items") -> None:
        self.path = Path(path)
        self.table = table
        self._conn: sqlite3.Connection | None = None
        self.logger = get_logger(component="SQLitePipeline")

    async def open(self, spider: "Spider") -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path)
        cur = self._conn.cursor()
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                spider TEXT NOT NULL,
                data   TEXT NOT NULL
            )
            """
        )
        self._conn.commit()
        self.logger.info(
            "Opened SQLite pipeline", path=str(self.path), table=self.table
        )

    async def close(self, spider: "Spider") -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
            self.logger.info("Closed SQLite pipeline", path=str(self.path))

    async def process_item(self, item: Any, spider: "Spider") -> Any:
        if not self._conn:
            raise RuntimeError("SQLitePipeline not opened")
        cur = self._conn.cursor()
        cur.execute(
            f"INSERT INTO {self.table} (spider, data) VALUES (?, ?)",
            (spider.name, json.dumps(item)),
        )
        self._conn.commit()
        self.logger.debug("Stored item in SQLite", table=self.table, spider=spider.name)
        return item
