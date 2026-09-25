from __future__ import annotations

import json
import sys
from typing import TYPE_CHECKING

try:
    # Skip on Windows - cassandra-driver requires libev C extension which is not available
    if sys.platform == "win32":
        msg = "cassandra-driver not supported on Windows"
        raise ImportError(msg)
    from cassandra.auth import (  # pyright: ignore[reportMissingImports]
        PlainTextAuthProvider,
    )
    from cassandra.cluster import (  # pyright: ignore[reportMissingImports]
        Cluster,
    )

    CASSANDRA_AVAILABLE = True
except ImportError:
    Cluster = None  # type: ignore
    PlainTextAuthProvider = None  # type: ignore
    CASSANDRA_AVAILABLE = False

from ..logging import get_logger
from .base import log_pipeline_item, validate_table_name

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class CassandraPipeline:
    """
    Pipeline that sends items to an Apache Cassandra database.

    Example:
        from silkworm.pipelines import CassandraPipeline

        pipeline = CassandraPipeline(
            hosts=["127.0.0.1"],
            keyspace="scraping",
            table="items",
            username="cassandra",
            password="cassandra",
        )
    """

    def __init__(
        self,
        hosts: list[str] | None = None,
        keyspace: str = "scraping",
        *,
        table: str = "items",
        username: str | None = None,
        password: str | None = None,
        port: int = 9042,
    ) -> None:
        """
        Initialize CassandraPipeline.

        Args:
            hosts: List of Cassandra cluster hosts (default: ["127.0.0.1"])
            keyspace: Keyspace name
            table: Table name (default: "items")
            username: Optional username for authentication
            password: Optional password for authentication
            port: Cassandra port (default: 9042)
        """
        if not CASSANDRA_AVAILABLE:
            raise ImportError(
                "cassandra-driver is required for CassandraPipeline. "
                "Install it with: pip install silkworm-rs[cassandra]",
            )

        self.hosts = hosts or ["127.0.0.1"]
        self.keyspace = keyspace
        self.table = validate_table_name(table)
        self.username = username
        self.password = password
        self.port = port
        self._cluster = None  # type: ignore[var-annotated]
        self._session = None  # type: ignore[var-annotated]
        self.logger = get_logger(component="CassandraPipeline")

    async def open(self, spider: Spider) -> None:
        # Setup authentication if credentials provided
        auth_provider = None
        if self.username and self.password:
            auth_provider = PlainTextAuthProvider(  # type: ignore[misc]
                username=self.username,
                password=self.password,
            )

        # Connect to Cassandra cluster
        cluster = Cluster(  # type: ignore[misc]
            self.hosts,
            port=self.port,
            auth_provider=auth_provider,
        )
        session = cluster.connect()
        self._cluster = cluster
        self._session = session

        # Create keyspace if it doesn't exist
        session.execute(
            f"""
            CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """,
        )

        # Use the keyspace
        session.set_keyspace(self.keyspace)

        # Create table if it doesn't exist
        session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                id uuid PRIMARY KEY,
                spider text,
                data text,
                created_at timestamp
            )
            """,
        )

        self.logger.info(
            "Opened Cassandra pipeline",
            hosts=self.hosts,
            keyspace=self.keyspace,
            table=self.table,
        )

    async def close(self, spider: Spider) -> None:
        if self._cluster:
            self._cluster.shutdown()
            self._cluster = None
            self._session = None
            self.logger.info("Closed Cassandra pipeline", table=self.table)

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        if not self._session:
            raise RuntimeError("CassandraPipeline not opened")

        import uuid
        from datetime import UTC, datetime

        # Insert item into Cassandra
        self._session.execute(
            f"""
            INSERT INTO {self.table} (id, spider, data, created_at)
            VALUES (%s, %s, %s, %s)
            """,
            (
                uuid.uuid4(),
                spider.name,
                json.dumps(item, ensure_ascii=False),
                datetime.now(UTC),
            ),
        )

        log_pipeline_item(
            self,
            "Inserted item in Cassandra",
            table=self.table,
            spider=spider.name,
        )
        return item
