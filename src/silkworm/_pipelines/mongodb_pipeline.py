from __future__ import annotations

from typing import TYPE_CHECKING

try:
    import motor.motor_asyncio  # type: ignore[import-not-found]

    MOTOR_AVAILABLE = True
except ImportError:
    MOTOR_AVAILABLE = False

from ..logging import Logger, get_logger
from .base import log_pipeline_item

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class MongoDBPipeline:
    """
    Pipeline that sends items to a MongoDB collection.

    Args:
        connection_string: MongoDB connection URI.
        database: Database name.
        collection: Collection receiving documents.

    Example::

        from silkworm.pipelines import MongoDBPipeline

        pipeline = MongoDBPipeline(
            connection_string="mongodb://localhost:27017",
            database="scraping",
            collection="quotes",
        )
    """

    def __init__(
        self,
        connection_string: str = "mongodb://localhost:27017",
        *,
        database: str = "scraping",
        collection: str = "items",
    ) -> None:
        """
        Initialize MongoDBPipeline.

        Args:
            connection_string: MongoDB connection string
            database: Database name
            collection: Collection name
        """
        if not MOTOR_AVAILABLE:
            raise ImportError(
                "motor is required for MongoDBPipeline. Install it with: pip install silkworm-rs[mongodb]",
            )

        self.connection_string = connection_string
        self.database = database
        self.collection = collection
        self._client = None
        self._db = None
        self._coll = None
        self.logger: Logger = get_logger(component="MongoDBPipeline")

    async def open(self, spider: Spider) -> None:
        """Create the Motor client and select the database and collection."""
        self._client = motor.motor_asyncio.AsyncIOMotorClient(self.connection_string)  # type: ignore[assignment]
        self._db = self._client[self.database]
        self._coll = self._db[self.collection]
        self.logger.info(
            "Opened MongoDB pipeline",
            database=self.database,
            collection=self.collection,
        )

    async def close(self, spider: Spider) -> None:
        """Close the MongoDB client and release collection references."""
        client = self._client
        self._client = None
        self._db = None
        self._coll = None
        if client:
            client.close()
            self.logger.info("Closed MongoDB pipeline", collection=self.collection)

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        """Insert a shallow copy so MongoDB cannot add ``_id`` to the item."""
        if self._coll is None:
            raise RuntimeError("MongoDBPipeline not opened")

        # Make a shallow copy to avoid mutating the original item when MongoDB adds _id.
        # Shallow copy is sufficient since MongoDB only adds _id at the root level.
        item_copy = dict(item) if isinstance(item, dict) else item
        await self._coll.insert_one(item_copy)
        log_pipeline_item(
            self,
            "Inserted item in MongoDB",
            collection=self.collection,
            spider=spider.name,
        )
        return item
