from __future__ import annotations

from typing import TYPE_CHECKING, Any

try:
    from elasticsearch import AsyncElasticsearch  # type: ignore[import-not-found]

    ELASTICSEARCH_AVAILABLE = True
except ImportError:
    ELASTICSEARCH_AVAILABLE = False

from ..logging import Logger, get_logger
from .base import log_pipeline_item

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class ElasticsearchPipeline:
    """
    Pipeline that sends items to an Elasticsearch index.

    Args:
        hosts: One endpoint or a list of Elasticsearch endpoints.
        index: Destination index name.
        **es_kwargs: Additional ``AsyncElasticsearch`` client options.

    Example::

        from silkworm.pipelines import ElasticsearchPipeline

        pipeline = ElasticsearchPipeline(
            hosts=["http://localhost:9200"],
            index="quotes",
        )
    """

    def __init__(
        self,
        hosts: list[str] | str = "http://localhost:9200",
        *,
        index: str = "items",
        **es_kwargs: Any,
    ) -> None:
        """
        Initialize ElasticsearchPipeline.

        Args:
            hosts: Elasticsearch host(s)
            index: Index name
            **es_kwargs: Additional kwargs for AsyncElasticsearch client
        """
        if not ELASTICSEARCH_AVAILABLE:
            raise ImportError(
                "elasticsearch is required for ElasticsearchPipeline. Install it with: pip install silkworm-rs[elasticsearch]",
            )

        self.hosts: list[str] = [hosts] if isinstance(hosts, str) else hosts
        self.index = index
        self.es_kwargs: dict[str, Any] = es_kwargs
        self._client: AsyncElasticsearch | None = None
        self.logger: Logger = get_logger(component="ElasticsearchPipeline")

    async def open(self, spider: Spider) -> None:
        """Create the asynchronous Elasticsearch client."""
        self._client = AsyncElasticsearch(self.hosts, **self.es_kwargs)  # pyright: ignore[reportPossiblyUnboundVariable]
        self.logger.info(
            "Opened Elasticsearch pipeline",
            hosts=self.hosts,
            index=self.index,
        )

    async def close(self, spider: Spider) -> None:
        """Close the Elasticsearch transport and release the client."""
        client = self._client
        self._client = None
        if client:
            await client.close()
            self.logger.info("Closed Elasticsearch pipeline", index=self.index)

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        """Index one item as a document in the configured index."""
        if not self._client:
            raise RuntimeError("ElasticsearchPipeline not opened")

        await self._client.index(index=self.index, document=item)  # type: ignore[arg-type]
        log_pipeline_item(
            self,
            "Indexed item in Elasticsearch",
            index=self.index,
            spider=spider.name,
        )
        return item
