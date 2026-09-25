from __future__ import annotations

import json
from typing import TYPE_CHECKING

try:
    import opendal  # type: ignore[import-not-found]

    OPENDAL_AVAILABLE = True
except ImportError:
    OPENDAL_AVAILABLE = False

from ..logging import get_logger
from .base import _log_pipeline_item

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class S3JsonLinesPipeline:
    """
    Pipeline that writes items to S3 in JSON Lines format using async OpenDAL.

    Example:
        from silkworm.pipelines import S3JsonLinesPipeline

        pipeline = S3JsonLinesPipeline(
            bucket="my-bucket",
            key="data/items.jl",
            region="us-east-1",
        )
    """

    def __init__(
        self,
        bucket: str,
        key: str = "items.jl",
        *,
        region: str = "us-east-1",
        endpoint: str | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
    ) -> None:
        """
        Initialize S3JsonLinesPipeline.

        Args:
            bucket: S3 bucket name
            key: S3 object key (path)
            region: AWS region
            endpoint: Custom S3 endpoint (for S3-compatible services)
            access_key_id: AWS access key ID (uses env vars if not provided)
            secret_access_key: AWS secret access key (uses env vars if not provided)
        """
        if not OPENDAL_AVAILABLE:
            raise ImportError(
                "opendal is required for S3JsonLinesPipeline. Install it with: pip install silkworm-rs[s3]",
            )

        self.bucket = bucket
        self.key = key
        self.region = region
        self.endpoint = endpoint
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self._items: list[str] = []
        self._operator: opendal.AsyncOperator | None = None
        self.logger = get_logger(component="S3JsonLinesPipeline")

    async def open(self, spider: Spider) -> None:
        # Configure OpenDAL operator for S3
        config = {
            "bucket": self.bucket,
            "region": self.region,
        }
        if self.endpoint:
            config["endpoint"] = self.endpoint
        if self.access_key_id:
            config["access_key_id"] = self.access_key_id
        if self.secret_access_key:
            config["secret_access_key"] = self.secret_access_key

        self._operator = opendal.AsyncOperator("s3", **config)  # pyright: ignore[reportPossiblyUnboundVariable]
        self._items = []
        self.logger.info(
            "Opened S3 JSON Lines pipeline",
            bucket=self.bucket,
            key=self.key,
            region=self.region,
        )

    async def close(self, spider: Spider) -> None:
        if self._items and self._operator:
            # Write all buffered items to S3
            content = "\n".join(self._items)
            await self._operator.write(self.key, content.encode("utf-8"))
        self._operator = None
        self.logger.info("Closed S3 JSON Lines pipeline", key=self.key)

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        line = json.dumps(item, ensure_ascii=False)
        self._items.append(line)
        _log_pipeline_item(
            self, "Buffered item for S3", key=self.key, spider=spider.name
        )
        return item
