from __future__ import annotations

from typing import TYPE_CHECKING

try:
    from elasticsearch import AsyncElasticsearch  # type: ignore[import-not-found]

    ELASTICSEARCH_AVAILABLE = True
except ImportError:
    ELASTICSEARCH_AVAILABLE = False

from ..logging import get_logger
from .base import _log_pipeline_item

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class ElasticsearchPipeline:
    """
    Pipeline that sends items to an Elasticsearch index.

    Example:
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
        **es_kwargs,
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

        self.hosts = [hosts] if isinstance(hosts, str) else hosts
        self.index = index
        self.es_kwargs = es_kwargs
        self._client: AsyncElasticsearch | None = None
        self.logger = get_logger(component="ElasticsearchPipeline")

    async def open(self, spider: Spider) -> None:
        self._client = AsyncElasticsearch(self.hosts, **self.es_kwargs)  # pyright: ignore[reportPossiblyUnboundVariable]
        self.logger.info(
            "Opened Elasticsearch pipeline",
            hosts=self.hosts,
            index=self.index,
        )

    async def close(self, spider: Spider) -> None:
        if self._client:
            await self._client.close()
            self._client = None
            self.logger.info("Closed Elasticsearch pipeline", index=self.index)

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        if not self._client:
            raise RuntimeError("ElasticsearchPipeline not opened")

        await self._client.index(index=self.index, document=item)  # type: ignore[arg-type]
        _log_pipeline_item(
            self,
            "Indexed item in Elasticsearch",
            index=self.index,
            spider=spider.name,
        )
        return item
