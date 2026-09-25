from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

try:
    import snowflake.connector  # type: ignore[import-not-found]

    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False

from ..logging import Logger, get_logger
from .base import log_pipeline_item, validate_table_name

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class SnowflakePipeline:
    """
    Pipeline that sends items to a Snowflake data warehouse.

    Example:
        from silkworm.pipelines import SnowflakePipeline

        pipeline = SnowflakePipeline(
            account="myaccount",
            user="myuser",
            password="mypassword",
            database="mydatabase",
            schema="myschema",
            warehouse="mywarehouse",
            table="items",
        )
    """

    def __init__(
        self,
        account: str,
        user: str,
        password: str,
        database: str,
        schema: str,
        warehouse: str,
        *,
        table: str = "items",
        role: str | None = None,
    ) -> None:
        """
        Initialize SnowflakePipeline.

        Args:
            account: Snowflake account identifier
            user: Snowflake username
            password: Snowflake password
            database: Database name
            schema: Schema name
            warehouse: Warehouse name
            table: Table name (default: "items")
            role: Optional role name
        """
        if not SNOWFLAKE_AVAILABLE:
            raise ImportError(
                "snowflake-connector-python is required for SnowflakePipeline. "
                "Install it with: pip install silkworm-rs[snowflake]",
            )

        self.account = account
        self.user = user
        self.password = password
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.table: str = validate_table_name(table)
        self.role = role
        self._conn: Any = None
        self._cursor: Any = None
        self.logger: Logger = get_logger(component="SnowflakePipeline")

    async def open(self, spider: Spider) -> None:
        # Connect to Snowflake
        conn_params = {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "schema": self.schema,
            "warehouse": self.warehouse,
        }
        if self.role:
            conn_params["role"] = self.role

        conn = snowflake.connector.connect(**conn_params)  # type: ignore[attr-defined]
        cursor = conn.cursor()
        self._conn = conn
        self._cursor = cursor

        # Create table if it doesn't exist
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                id NUMBER AUTOINCREMENT PRIMARY KEY,
                spider VARCHAR(255) NOT NULL,
                data VARIANT NOT NULL,
                created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """,
        )

        self.logger.info(
            "Opened Snowflake pipeline",
            account=self.account,
            database=self.database,
            schema=self.schema,
            table=self.table,
        )

    async def close(self, spider: Spider) -> None:
        if self._cursor:
            self._cursor.close()
            self._cursor = None

        if self._conn:
            self._conn.close()
            self._conn = None

        self.logger.info("Closed Snowflake pipeline", table=self.table)

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        if not self._cursor or not self._conn:
            raise RuntimeError("SnowflakePipeline not opened")

        # Insert item into Snowflake
        self._cursor.execute(
            f"INSERT INTO {self.table} (spider, data) VALUES (%s, %s)",
            (spider.name, json.dumps(item, ensure_ascii=False)),
        )
        self._conn.commit()

        log_pipeline_item(
            self,
            "Inserted item in Snowflake",
            table=self.table,
            spider=spider.name,
        )
        return item
