from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

try:
    import duckdb  # type: ignore[import-not-found]

    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

from ..logging import get_logger
from .base import log_pipeline_item, validate_table_name

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class DuckDBPipeline:
    """
    Pipeline that sends items to a DuckDB database.

    DuckDB is an embedded analytical database with excellent performance for OLAP queries.
    This pipeline stores items in a DuckDB table as JSON.

    Example:
        from silkworm.pipelines import DuckDBPipeline

        pipeline = DuckDBPipeline(
            database="data/scraping.db",
            table="items",
        )
    """

    def __init__(
        self,
        database: str | Path = "items.db",
        *,
        table: str = "items",
    ) -> None:
        """
        Initialize DuckDBPipeline.

        Args:
            database: Path to the DuckDB database file (default: "items.db")
            table: Table name (default: "items")
        """
        if not DUCKDB_AVAILABLE:
            raise ImportError(
                "duckdb is required for DuckDBPipeline. "
                "Install it with: pip install silkworm-rs[duckdb]",
            )

        self.database = Path(database)
        self.table = validate_table_name(table)
        self._conn: duckdb.DuckDBPyConnection | None = None
        self.logger = get_logger(component="DuckDBPipeline")

    async def open(self, spider: Spider) -> None:
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(str(self.database))  # type: ignore[attr-defined]
        assert self._conn is not None

        # Create sequence for auto-incrementing IDs
        # Use IF NOT EXISTS to avoid errors on reopening
        self._conn.execute(
            f"CREATE SEQUENCE IF NOT EXISTS {self.table}_seq START 1",
        )

        # Create table if it doesn't exist
        self._conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                id INTEGER PRIMARY KEY DEFAULT nextval('{self.table}_seq'),
                spider VARCHAR NOT NULL,
                data JSON NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
        )

        self.logger.info(
            "Opened DuckDB pipeline",
            database=str(self.database),
            table=self.table,
        )

    async def close(self, spider: Spider) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
            self.logger.info("Closed DuckDB pipeline", database=str(self.database))

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        if not self._conn:
            raise RuntimeError("DuckDBPipeline not opened")

        # Insert item into DuckDB
        # DuckDB's JSON type accepts JSON strings
        self._conn.execute(
            f"INSERT INTO {self.table} (spider, data) VALUES (?, ?)",
            (spider.name, json.dumps(item, ensure_ascii=False)),
        )

        log_pipeline_item(
            self,
            "Inserted item in DuckDB",
            table=self.table,
            spider=spider.name,
        )
        return item
