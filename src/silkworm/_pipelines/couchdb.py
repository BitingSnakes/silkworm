from __future__ import annotations

from typing import TYPE_CHECKING, Any

try:
    import aiocouch  # type: ignore[import-not-found]

    AIOCOUCH_AVAILABLE = True
except ImportError:
    AIOCOUCH_AVAILABLE = False

from ..logging import get_logger
from .base import _log_pipeline_item

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class CouchDBPipeline:
    """
    Pipeline that sends items to a CouchDB database.

    Example:
        from silkworm.pipelines import CouchDBPipeline

        pipeline = CouchDBPipeline(
            url="http://localhost:5984",
            database="scraping",
            username="admin",
            password="password",
        )
    """

    def __init__(
        self,
        url: str = "http://localhost:5984",
        database: str = "scraping",
        *,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        """
        Initialize CouchDBPipeline.

        Args:
            url: CouchDB server URL (default: "http://localhost:5984")
            database: Database name (default: "scraping")
            username: Optional username for authentication
            password: Optional password for authentication
        """
        if not AIOCOUCH_AVAILABLE:
            raise ImportError(
                "aiocouch is required for CouchDBPipeline. "
                "Install it with: pip install silkworm-rs[couchdb]",
            )

        self.url = url
        self.database = database
        self.username = username
        self.password = password
        self._client: Any = None
        self._db: Any = None
        self.logger = get_logger(component="CouchDBPipeline")

    async def open(self, spider: Spider) -> None:
        # Connect to CouchDB
        if self.username and self.password:
            self._client = await aiocouch.CouchDB(  # type: ignore[attr-defined]
                self.url,
                user=self.username,
                password=self.password,
            ).__aenter__()
        else:
            self._client = await aiocouch.CouchDB(self.url).__aenter__()  # type: ignore[attr-defined]

        client = self._client
        if client is None:
            raise RuntimeError("Failed to initialize CouchDB client")

        # Create database if it doesn't exist
        try:
            self._db = await client[self.database]  # type: ignore[index]
        except KeyError:
            self._db = await client.create(self.database)  # type: ignore[union-attr]

        self.logger.info(
            "Opened CouchDB pipeline",
            url=self.url,
            database=self.database,
        )

    async def close(self, spider: Spider) -> None:
        if self._client:
            await self._client.__aexit__(None, None, None)  # type: ignore[union-attr]
            self._client = None
            self._db = None
            self.logger.info("Closed CouchDB pipeline", database=self.database)

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        if not self._db:
            raise RuntimeError("CouchDBPipeline not opened")

        # Add spider name to item metadata
        doc_data = {"spider": spider.name, "data": item}

        # Create document in CouchDB
        await self._db.create(doc_data)  # type: ignore[union-attr]

        _log_pipeline_item(
            self,
            "Inserted item in CouchDB",
            database=self.database,
            spider=spider.name,
        )
        return item
