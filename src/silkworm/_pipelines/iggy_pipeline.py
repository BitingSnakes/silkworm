from __future__ import annotations

import json
from datetime import timedelta
from typing import TYPE_CHECKING

try:
    from apache_iggy import (  # type: ignore[import-not-found]
        IggyClient,
        Partitioning,
        SendMessage,
    )

    IGGY_AVAILABLE = True
except ImportError:
    IggyClient = None
    Partitioning = None
    SendMessage = None
    IGGY_AVAILABLE = False

from ..logging import Logger, get_logger
from .base import _BatchPipelineMixin, log_pipeline_item

if TYPE_CHECKING:
    from apache_iggy import (  # type: ignore[import-not-found]
        BackgroundProducerConfig,
        DirectProducerConfig,
        IggyProducer,
    )
    from apache_iggy import (  # type: ignore[import-not-found]
        IggyClient as _IggyClient,
    )
    from apache_iggy import (  # type: ignore[import-not-found]
        Partitioning as _Partitioning,
    )

    from .._types import JSONValue
    from ..spiders import Spider

_DEFAULT_CONNECTION_STRING = "iggy+tcp://iggy:iggy@127.0.0.1:8090"


class IggyPipeline(_BatchPipelineMixin):
    """
    Pipeline that publishes JSON-serialized items to Apache Iggy.

    Args:
        stream: Name of the destination Iggy stream.
        topic: Name of the destination topic.
        connection_string: Iggy connection string used to create a pipeline-owned
            client. Defaults to a local TCP server with Iggy's development
            credentials. Mutually exclusive with ``client``.
        client: Existing connected Iggy client to reuse. The pipeline does not
            connect or otherwise manage an injected client.
        partitioning: Iggy partitioning strategy. Balanced partitioning is used
            when omitted.
        mode: Direct or background producer configuration. Iggy's direct mode is
            used when omitted.
        create_stream_if_not_exists: Create ``stream`` during startup when needed.
        create_topic_if_not_exists: Create ``topic`` during startup when needed.
        topic_partitions_count: Number of partitions used when creating ``topic``.
        send_retries: Number of producer send retries, or ``None`` for unlimited
            retries.
        send_retry_interval: Delay between producer send retries.

    Each item is encoded as one compact UTF-8 JSON message. Closing the pipeline
    shuts down its producer and flushes messages accepted by a background producer.
    Use :meth:`process_items` to publish several items in one native Iggy batch.

    Example::

        from silkworm.pipelines import IggyPipeline

        pipeline = IggyPipeline("scraping", "items")
    """

    def __init__(
        self,
        stream: str,
        topic: str,
        *,
        connection_string: str | None = None,
        client: _IggyClient | None = None,
        partitioning: _Partitioning | None = None,
        mode: DirectProducerConfig | BackgroundProducerConfig | None = None,
        create_stream_if_not_exists: bool = True,
        create_topic_if_not_exists: bool = True,
        topic_partitions_count: int = 1,
        send_retries: int | None = 3,
        send_retry_interval: timedelta = timedelta(seconds=1),
    ) -> None:
        """Initialize an Apache Iggy producer pipeline."""
        if not IGGY_AVAILABLE:
            raise ImportError(
                "apache-iggy is required for IggyPipeline. "
                "Install it with: pip install silkworm-rs[iggy]",
            )
        if connection_string is not None and client is not None:
            raise ValueError("'connection_string' and 'client' are mutually exclusive")

        self.stream = stream
        self.topic = topic
        self.connection_string: str = (
            _DEFAULT_CONNECTION_STRING
            if connection_string is None
            else connection_string
        )
        self._provided_client = client
        self.partitioning = partitioning
        self.mode = mode
        self.create_stream_if_not_exists = create_stream_if_not_exists
        self.create_topic_if_not_exists = create_topic_if_not_exists
        self.topic_partitions_count = topic_partitions_count
        self.send_retries = send_retries
        self.send_retry_interval = send_retry_interval
        self._client: _IggyClient | None = None
        self._producer: IggyProducer | None = None
        self.logger: Logger = get_logger(component="IggyPipeline")

    async def open(self, spider: Spider) -> None:
        """Connect a pipeline-owned client and initialize the Iggy producer."""
        if self._producer is not None:
            raise RuntimeError("IggyPipeline already opened")

        client = self._provided_client
        owns_client = client is None
        if client is None:
            assert IggyClient is not None
            client = IggyClient.from_connection_string(self.connection_string)
            await client.connect()

        assert Partitioning is not None
        partitioning = (
            Partitioning.balanced() if self.partitioning is None else self.partitioning
        )
        try:
            producer = await client.producer(
                self.stream,
                self.topic,
                partitioning=partitioning,
                mode=self.mode,
                create_stream_if_not_exists=self.create_stream_if_not_exists,
                create_topic_if_not_exists=self.create_topic_if_not_exists,
                topic_partitions_count=self.topic_partitions_count,
                send_retries=self.send_retries,
                send_retry_interval=self.send_retry_interval,
            )
        except BaseException:
            self._client = None
            raise

        self._client = client
        self._producer = producer
        self.logger.info(
            "Opened Apache Iggy pipeline",
            stream=self.stream,
            topic=self.topic,
            owns_client=owns_client,
        )

    async def close(self, spider: Spider) -> None:
        """Flush and shut down the Iggy producer."""
        producer = self._producer
        self._producer = None
        self._client = None
        if producer is not None:
            await producer.shutdown()
        self.logger.info(
            "Closed Apache Iggy pipeline",
            stream=self.stream,
            topic=self.topic,
        )

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        """Publish one JSON item to Iggy and pass it to the next pipeline."""
        producer = self._producer
        if producer is None:
            raise RuntimeError("IggyPipeline not opened")

        payload = json.dumps(item, ensure_ascii=False, separators=(",", ":"))
        assert SendMessage is not None
        await producer.send_one(SendMessage(payload))
        log_pipeline_item(
            self,
            "Published item to Apache Iggy",
            stream=self.stream,
            topic=self.topic,
            spider=spider.name,
        )
        return item

    async def process_items(
        self,
        items: list[JSONValue],
        spider: Spider,
    ) -> list[JSONValue]:
        """Publish several JSON items in one Iggy batch and return them unchanged."""
        producer = self._producer
        if producer is None:
            raise RuntimeError("IggyPipeline not opened")
        if not items:
            return items

        assert SendMessage is not None
        messages = [
            SendMessage(json.dumps(item, ensure_ascii=False, separators=(",", ":")))
            for item in items
        ]
        await producer.send(messages)
        log_pipeline_item(
            self,
            "Published item batch to Apache Iggy",
            stream=self.stream,
            topic=self.topic,
            item_count=len(items),
            spider=spider.name,
        )
        return items
