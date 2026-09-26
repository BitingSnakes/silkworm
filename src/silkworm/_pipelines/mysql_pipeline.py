from __future__ import annotations

import json
from typing import TYPE_CHECKING

try:
    import aiomysql  # type: ignore[import-not-found, import-untyped]

    AIOMYSQL_AVAILABLE = True
except ImportError:
    AIOMYSQL_AVAILABLE = False

from .._resources import raise_cleanup_errors
from ..logging import Logger, get_logger
from .base import log_pipeline_item, validate_table_name

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class MySQLPipeline:
    """
    Pipeline that sends items to a MySQL database.

    Args:
        host: Database host.
        port: Database port.
        user: Login user.
        password: Login password.
        database: Existing database name.
        table: Valid unquoted table created automatically when absent.

    Example::

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
        self.table: str = validate_table_name(table)
        self._pool = None
        self.logger: Logger = get_logger(component="MySQLPipeline")

    async def open(self, spider: Spider) -> None:
        """Create the connection pool and JSON table if absent."""
        pool = await aiomysql.create_pool(  # pyright: ignore[reportPossiblyUnboundVariable]
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.database,
        )
        self._pool = pool

        try:
            # Create table if it doesn't exist
            async with (
                pool.acquire() as conn,
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
        except BaseException as exc:
            self._pool = None
            try:
                await self._close_pool(pool)
            except BaseException as cleanup_exc:  # noqa: BLE001
                exc.add_note(f"MySQL rollback failed: {cleanup_exc}")
            raise

        self.logger.info(
            "Opened MySQL pipeline",
            host=self.host,
            database=self.database,
            table=self.table,
        )

    async def close(self, spider: Spider) -> None:
        """Close the MySQL connection pool and wait for shutdown."""
        pool = self._pool
        self._pool = None
        if pool:
            await self._close_pool(pool)
            self.logger.info("Closed MySQL pipeline", table=self.table)

    async def _close_pool(self, pool: object) -> None:
        errors: list[BaseException] = []
        try:
            pool.close()  # type: ignore[attr-defined]
        except BaseException as exc:  # noqa: BLE001 - report all pool failures
            errors.append(exc)
        try:
            await pool.wait_closed()  # type: ignore[attr-defined]
        except BaseException as exc:  # noqa: BLE001 - attempt both pool hooks
            errors.append(exc)
        raise_cleanup_errors("MySQL pool cleanup failed", errors)

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        """Insert and commit one JSON item with its spider name."""
        if not self._pool:
            raise RuntimeError("MySQLPipeline not opened")

        async with (
            self._pool.acquire() as conn,
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
