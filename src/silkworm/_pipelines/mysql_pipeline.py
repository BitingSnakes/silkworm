from __future__ import annotations

import json
from typing import TYPE_CHECKING

try:
    import aiomysql  # type: ignore[import-not-found, import-untyped]

    AIOMYSQL_AVAILABLE = True
except ImportError:
    AIOMYSQL_AVAILABLE = False

from ..logging import get_logger
from .base import log_pipeline_item, validate_table_name

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class MySQLPipeline:
    """
    Pipeline that sends items to a MySQL database.

    Example:
        from silkworm.pipelines import MySQLPipeline

        pipeline = MySQLPipeline(
            host="localhost",
            port=3306,
            user="root",
            password="password",
            database="scraping",
            table="items",
        )
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 3306,
        user: str = "root",
        password: str = "",
        database: str = "scraping",
        *,
        table: str = "items",
    ) -> None:
        """
        Initialize MySQLPipeline.

        Args:
            host: MySQL host
            port: MySQL port
            user: MySQL user
            password: MySQL password
            database: Database name
            table: Table name
        """
        if not AIOMYSQL_AVAILABLE:
            raise ImportError(
                "aiomysql is required for MySQLPipeline. Install it with: pip install silkworm-rs[mysql]",
            )

        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.table = validate_table_name(table)
        self._pool = None  # type: ignore[var-annotated]
        self.logger = get_logger(component="MySQLPipeline")

    async def open(self, spider: Spider) -> None:
        self._pool = await aiomysql.create_pool(  # pyright: ignore[reportPossiblyUnboundVariable]
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.database,
        )

        # Create table if it doesn't exist
        async with (
            self._pool.acquire() as conn,  # type: ignore[union-attr, attr-defined]
            conn.cursor() as cur,
        ):
            await cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    spider VARCHAR(255) NOT NULL,
                    data JSON NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,
            )
            await conn.commit()

        self.logger.info(
            "Opened MySQL pipeline",
            host=self.host,
            database=self.database,
            table=self.table,
        )

    async def close(self, spider: Spider) -> None:
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None
            self.logger.info("Closed MySQL pipeline", table=self.table)

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        if not self._pool:
            raise RuntimeError("MySQLPipeline not opened")

        async with (
            self._pool.acquire() as conn,  # type: ignore[union-attr, attr-defined]
            conn.cursor() as cur,
        ):
            await cur.execute(
                f"INSERT INTO {self.table} (spider, data) VALUES (%s, %s)",
                (spider.name, json.dumps(item, ensure_ascii=False)),
            )
            await conn.commit()

        log_pipeline_item(
            self,
            "Inserted item in MySQL",
            table=self.table,
            spider=spider.name,
        )
        return item
