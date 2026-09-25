from __future__ import annotations

import json
from typing import TYPE_CHECKING

try:
    import aioboto3  # type: ignore[import-not-found, import-untyped]

    AIOBOTO3_AVAILABLE = True
except ImportError:
    AIOBOTO3_AVAILABLE = False

from ..logging import Logger, get_logger
from .base import log_pipeline_item

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class DynamoDBPipeline:
    """
    Pipeline that sends items to AWS DynamoDB.

    Args:
        table_name: Table opened or created with a string ``id`` hash key.
        region_name: AWS region.
        aws_access_key_id: Explicit access key, or ``None`` for normal provider
            discovery.
        aws_secret_access_key: Explicit secret paired with the access key.
        endpoint_url: Custom endpoint for DynamoDB Local or compatible services.

    Example:
        from silkworm.pipelines import DynamoDBPipeline

        pipeline = DynamoDBPipeline(
            table_name="items",
            region_name="us-east-1",
            aws_access_key_id="YOUR_KEY",
            aws_secret_access_key="YOUR_SECRET",
        )
    """

    def __init__(
        self,
        table_name: str = "items",
        *,
        region_name: str = "us-east-1",
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        endpoint_url: str | None = None,
    ) -> None:
        """
        Initialize DynamoDBPipeline.

        Args:
            table_name: DynamoDB table name (default: "items")
            region_name: AWS region (default: "us-east-1")
            aws_access_key_id: AWS access key ID (uses env vars/IAM if not provided)
            aws_secret_access_key: AWS secret access key (uses env vars/IAM if not provided)
            endpoint_url: Custom endpoint URL for DynamoDB Local or other services
        """
        if not AIOBOTO3_AVAILABLE:
            raise ImportError(
                "aioboto3 is required for DynamoDBPipeline. "
                "Install it with: pip install silkworm-rs[dynamodb]",
            )

        self.table_name = table_name
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.endpoint_url = endpoint_url
        self._session = None
        self._client = None
        self._resource = None
        self._table = None
        self.logger: Logger = get_logger(component="DynamoDBPipeline")

    async def open(self, spider: Spider) -> None:
        """Open DynamoDB resources and create the keyed table if absent."""
        # Create aioboto3 session
        session_kwargs = {"region_name": self.region_name}
        if self.aws_access_key_id and self.aws_secret_access_key:
            session_kwargs["aws_access_key_id"] = self.aws_access_key_id
            session_kwargs["aws_secret_access_key"] = self.aws_secret_access_key

        session = aioboto3.Session(**session_kwargs)  # type: ignore[attr-defined]
        self._session = session

        # Create resource and client
        resource_kwargs = {}
        if self.endpoint_url:
            resource_kwargs["endpoint_url"] = self.endpoint_url

        resource = await session.resource(
            "dynamodb",
            **resource_kwargs,
        ).__aenter__()
        client = await session.client(
            "dynamodb",
            **resource_kwargs,
        ).__aenter__()
        self._resource = resource
        self._client = client

        # Create table if it doesn't exist
        try:
            await client.describe_table(TableName=self.table_name)
            table = await resource.Table(self.table_name)
        except client.exceptions.ResourceNotFoundException:
            # Create table with a simple schema (id as primary key)
            table = await resource.create_table(
                TableName=self.table_name,
                KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
            # Wait for table to be created
            await table.wait_until_exists()
        self._table = table

        self.logger.info(
            "Opened DynamoDB pipeline",
            table_name=self.table_name,
            region=self.region_name,
        )

    async def close(self, spider: Spider) -> None:
        """Exit the DynamoDB client and resource contexts."""
        if self._client:
            await self._client.__aexit__(None, None, None)
            self._client = None
        if self._resource:
            await self._resource.__aexit__(None, None, None)
            self._resource = None
            self._table = None
        self.logger.info("Closed DynamoDB pipeline", table_name=self.table_name)

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        """Store one item with a generated string ID and spider metadata."""
        if not self._table:
            raise RuntimeError("DynamoDBPipeline not opened")

        import uuid

        # Create item with unique ID and spider metadata
        dynamo_item = {
            "id": str(uuid.uuid4()),
            "spider": spider.name,
            "data": json.dumps(item, ensure_ascii=False),
        }

        # Put item in DynamoDB
        await self._table.put_item(Item=dynamo_item)

        log_pipeline_item(
            self,
            "Inserted item in DynamoDB",
            table_name=self.table_name,
            spider=spider.name,
        )
        return item
