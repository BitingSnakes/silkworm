from __future__ import annotations

import json
from typing import TYPE_CHECKING

try:
    import asyncpg  # type: ignore[import-not-found, import-untyped]

    ASYNCPG_AVAILABLE = True
except ImportError:
    ASYNCPG_AVAILABLE = False

from ..logging import Logger, get_logger
from .base import log_pipeline_item, validate_table_name

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class PostgreSQLPipeline:
    """
    Pipeline that sends items to a PostgreSQL database.

    Args:
        host: Database host.
        port: Database port.
        user: Login user.
        password: Login password.
        database: Existing database name.
        table: Valid unquoted table created automatically when absent.

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
        self.table: str = validate_table_name(table)
        self._pool = None
        self.logger: Logger = get_logger(component="PostgreSQLPipeline")

    async def open(self, spider: Spider) -> None:
        """Create the async pool and JSONB table if absent."""
        pool = await asyncpg.create_pool(  # pyright: ignore[reportPossiblyUnboundVariable]
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )
        self._pool = pool

        try:
            # Create table if it doesn't exist
            async with pool.acquire() as conn:
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
        except BaseException as exc:
            self._pool = None
            try:
                await pool.close()
            except BaseException as cleanup_exc:  # noqa: BLE001
                exc.add_note(f"PostgreSQL rollback failed: {cleanup_exc}")
            raise

        self.logger.info(
            "Opened PostgreSQL pipeline",
            host=self.host,
            database=self.database,
            table=self.table,
        )

    async def close(self, spider: Spider) -> None:
        """Close the PostgreSQL connection pool."""
        pool = self._pool
        self._pool = None
        if pool:
            await pool.close()
            self.logger.info("Closed PostgreSQL pipeline", table=self.table)

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        """Insert one JSONB item with its spider name."""
        if not self._pool:
            raise RuntimeError("PostgreSQLPipeline not opened")

        async with self._pool.acquire() as conn:
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
