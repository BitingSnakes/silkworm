from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import TYPE_CHECKING

from ..logging import Logger, get_logger
from .base import log_pipeline_item, validate_table_name

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class SQLitePipeline:
    """Stream items into a SQLite table as JSON documents.

    Args:
        path: SQLite database path.
        table: Valid unquoted table name created automatically when absent.
    """

    def __init__(self, path: str | Path = "items.db", table: str = "items") -> None:
        self.path: Path = Path(path)
        self.table: str = validate_table_name(table)
        self._conn: sqlite3.Connection | None = None
        self.logger: Logger = get_logger(component="SQLitePipeline")

    async def open(self, spider: Spider) -> None:
        """Open the database and create the destination table if needed."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path)
        self._conn = conn
        try:
            with closing(conn.cursor()) as cur:
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.table} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        spider TEXT NOT NULL,
                        data   TEXT NOT NULL
                    )
                    """,
                )
            conn.commit()
        except BaseException as exc:
            self._conn = None
            try:
                conn.close()
            except BaseException as cleanup_exc:  # noqa: BLE001
                exc.add_note(f"SQLite rollback failed: {cleanup_exc}")
            raise
        self.logger.info(
            "Opened SQLite pipeline",
            path=str(self.path),
            table=self.table,
        )

    async def close(self, spider: Spider) -> None:
        """Commit outstanding writes and close the database connection."""
        conn = self._conn
        self._conn = None
        if conn:
            conn.close()
            self.logger.info("Closed SQLite pipeline", path=str(self.path))

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        """Insert one JSON-serialized item and return it unchanged."""
        if not self._conn:
            raise RuntimeError("SQLitePipeline not opened")
        with closing(self._conn.cursor()) as cur:
            cur.execute(
                f"INSERT INTO {self.table} (spider, data) VALUES (?, ?)",
                (spider.name, json.dumps(item, ensure_ascii=False)),
            )
        self._conn.commit()
        log_pipeline_item(
            self, "Stored item in SQLite", table=self.table, spider=spider.name
        )
        return item
