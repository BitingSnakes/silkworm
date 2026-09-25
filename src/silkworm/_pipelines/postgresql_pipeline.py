from __future__ import annotations

import json
from typing import TYPE_CHECKING

try:
    import asyncpg  # type: ignore[import-not-found, import-untyped]

    ASYNCPG_AVAILABLE = True
except ImportError:
    ASYNCPG_AVAILABLE = False

from ..logging import get_logger
from .base import log_pipeline_item, validate_table_name

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class PostgreSQLPipeline:
    """
    Pipeline that sends items to a PostgreSQL database.

    Example:
        from silkworm.pipelines import PostgreSQLPipeline

        pipeline = PostgreSQLPipeline(
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            database="scraping",
            table="items",
        )
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        user: str = "postgres",
        password: str = "",
        database: str = "scraping",
        *,
        table: str = "items",
    ) -> None:
        """
        Initialize PostgreSQLPipeline.

        Args:
            host: PostgreSQL host
            port: PostgreSQL port
            user: PostgreSQL user
            password: PostgreSQL password
            database: Database name
            table: Table name
        """
        if not ASYNCPG_AVAILABLE:
            raise ImportError(
                "asyncpg is required for PostgreSQLPipeline. Install it with: pip install silkworm-rs[postgresql]",
            )

        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.table = validate_table_name(table)
        self._pool = None  # type: ignore[var-annotated]
        self.logger = get_logger(component="PostgreSQLPipeline")

    async def open(self, spider: Spider) -> None:
        self._pool = await asyncpg.create_pool(  # pyright: ignore[reportPossiblyUnboundVariable]
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )

        # Create table if it doesn't exist
        async with self._pool.acquire() as conn:  # type: ignore[union-attr, attr-defined]
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table} (
                    id SERIAL PRIMARY KEY,
                    spider VARCHAR(255) NOT NULL,
                    data JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,
            )

        self.logger.info(
            "Opened PostgreSQL pipeline",
            host=self.host,
            database=self.database,
            table=self.table,
        )

    async def close(self, spider: Spider) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None
            self.logger.info("Closed PostgreSQL pipeline", table=self.table)

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        if not self._pool:
            raise RuntimeError("PostgreSQLPipeline not opened")

        async with self._pool.acquire() as conn:  # type: ignore[union-attr, attr-defined]
            await conn.execute(
                f"INSERT INTO {self.table} (spider, data) VALUES ($1, $2)",
                spider.name,
                json.dumps(item, ensure_ascii=False),
            )

        log_pipeline_item(
            self,
            "Inserted item in PostgreSQL",
            table=self.table,
            spider=spider.name,
        )
        return item
