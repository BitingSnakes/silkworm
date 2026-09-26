"""Type-level checks for the public API.

``assert_type`` is verified by the type checker (``just pyright``) and is a
no-op at runtime, so these tests also run under pytest.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta
from typing import assert_type

from silkworm import (
    EngineOptions,
    HTMLResponse,
    Request,
    Response,
    Spider,
    crawl,
    get_logger,
    run_spider,
)
from silkworm.pipelines import (
    CallbackPipeline,
    ItemPipeline,
    JsonLinesPipeline,
    ZenohKeyResolver,
)
from silkworm.types import (
    Callback,
    Headers,
    ItemCallback,
    JSONLike,
    JSONValue,
    Logger,
    MetaData,
    QueryParams,
)


class ArticleSpider(Spider):
    name = "articles"

    def __init__(self, *, section: str, **kwargs: object) -> None:
        super().__init__()
        self.section = section

    async def parse(self, response: Response) -> AsyncIterator[Request | JSONLike]:
        if isinstance(response, HTMLResponse):
            # Plain dict literals are valid callback output.
            yield {"title": response.url, "section": self.section}
            yield response.follow("/next", callback=self.parse)


def test_logger_types() -> None:
    assert_type(get_logger(component="test"), Logger)
    assert_type(ArticleSpider(section="news").log, Logger)


def test_request_field_types() -> None:
    request = Request(url="https://example.com", meta={"page": 1})
    assert_type(request.headers, Headers)
    assert_type(request.params, QueryParams)
    assert_type(request.meta, MetaData)
    assert_type(request.json, JSONValue | None)
    assert_type(request.timeout, float | timedelta | None)
    assert_type(request.callback, Callback | None)
    assert_type(request.replace(url="https://example.org"), Request)


def test_engine_options_is_a_typed_dict() -> None:
    options: EngineOptions = {
        "concurrency": 8,
        "request_timeout": timedelta(seconds=5),
        "emulation": None,
        "item_pipelines": [JsonLinesPipeline("items.jl")],
    }
    assert_type(options.get("concurrency"), int | None)


def test_runners_accept_spider_classes_and_instances() -> None:
    # Only referenced, never called: these lines are checked by pyright.
    def _type_check_only() -> None:
        run_spider(ArticleSpider(section="news"), concurrency=4)
        run_spider(Spider, request_timeout=10, keep_alive=True)
        _ = crawl(ArticleSpider(section="news"), emulation=None)

    assert callable(_type_check_only)


def test_callback_and_pipeline_protocols() -> None:
    def keep(item: JSONValue, spider: Spider) -> JSONValue:
        return item

    async def drop(item: JSONValue, spider: Spider) -> None:
        return None

    callbacks: list[ItemCallback] = [keep, drop]
    pipelines: list[ItemPipeline] = [
        CallbackPipeline(callback=keep),
        JsonLinesPipeline("items.jl"),
    ]
    assert len(callbacks) == len(pipelines) == 2


def test_zenoh_key_resolver_type() -> None:
    async def resolve(item: JSONValue, spider: Spider) -> str:
        return f"{spider.name}/{item}"

    resolver: ZenohKeyResolver = resolve
    assert callable(resolver)
