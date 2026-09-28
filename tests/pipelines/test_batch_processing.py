from __future__ import annotations

import inspect

import pytest

import silkworm.pipelines as pipeline_module
from silkworm.pipelines import CallbackPipeline, LoggedPipeline
from silkworm.spiders import Spider
from silkworm.types import JSONValue


def test_all_public_pipeline_classes_support_batch_processing() -> None:
    missing = [
        name
        for name in pipeline_module.__all__
        if name.endswith("Pipeline")
        and name != "ItemPipeline"
        and inspect.isclass(pipeline := getattr(pipeline_module, name))
        and not hasattr(pipeline, "process_items")
    ]

    assert missing == []


async def test_default_batch_processing_preserves_order_and_transformations() -> None:
    seen: list[JSONValue] = []

    def transform(item: JSONValue, spider: Spider) -> JSONValue:
        seen.append(item)
        return {"position": len(seen), "item": item}

    pipeline = CallbackPipeline(transform)
    spider = Spider(name="batch")
    items: list[JSONValue] = [{"id": 1}, {"id": 2}, {"id": 3}]

    processed = await pipeline.process_items(items, spider)

    assert seen == items
    assert processed == [
        {"position": 1, "item": {"id": 1}},
        {"position": 2, "item": {"id": 2}},
        {"position": 3, "item": {"id": 3}},
    ]


async def test_default_batch_processing_stops_at_first_error() -> None:
    seen: list[JSONValue] = []

    def fail_on_second(item: JSONValue, spider: Spider) -> JSONValue:
        seen.append(item)
        if item == {"id": 2}:
            raise RuntimeError("batch item failed")
        return item

    pipeline = CallbackPipeline(fail_on_second)
    items: list[JSONValue] = [{"id": 1}, {"id": 2}, {"id": 3}]

    with pytest.raises(RuntimeError, match="batch item failed"):
        await pipeline.process_items(items, Spider())

    assert seen == items[:2]


async def test_default_batch_processing_accepts_an_empty_batch() -> None:
    pipeline = CallbackPipeline(lambda item, spider: item)

    assert await pipeline.process_items([], Spider()) == []


async def test_logged_pipeline_preserves_native_batch_processing() -> None:
    class NativeBatchPipeline:
        def __init__(self) -> None:
            self.log_level = "DEBUG"
            self.single_calls = 0
            self.batch_calls = 0

        async def open(self, spider: Spider) -> None:
            pass

        async def close(self, spider: Spider) -> None:
            pass

        async def process_item(
            self,
            item: JSONValue,
            spider: Spider,
        ) -> JSONValue:
            self.single_calls += 1
            return item

        async def process_items(
            self,
            items: list[JSONValue],
            spider: Spider,
        ) -> list[JSONValue]:
            self.batch_calls += 1
            return items

    native = NativeBatchPipeline()
    pipeline = LoggedPipeline(native, log_level="INFO")
    items: list[JSONValue] = [{"id": 1}, {"id": 2}]

    assert await pipeline.process_items(items, Spider()) is items
    assert native.batch_calls == 1
    assert native.single_calls == 0
    assert native.log_level == "INFO"
