from __future__ import annotations

import inspect
import json
import sqlite3

import pytest

import silkworm.pipelines as pipeline_module
from silkworm.engine import Engine
from silkworm.exceptions import DropItem, SpiderError
from silkworm.pipelines import (
    CallbackPipeline,
    JsonLinesPipeline,
    LoggedPipeline,
    SQLitePipeline,
)
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


async def test_engine_batches_sequential_emits() -> None:
    class NativeBatchPipeline:
        native_batch = True

        def __init__(self) -> None:
            self.batches: list[list[JSONValue]] = []

        async def open(self, spider: Spider) -> None:
            pass

        async def close(self, spider: Spider) -> None:
            pass

        async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
            raise AssertionError("single-item path used")

        async def process_items(
            self, items: list[JSONValue], spider: Spider
        ) -> list[JSONValue]:
            self.batches.append(items.copy())
            return items

    class BatchSpider(Spider):
        async def produce(self) -> None:
            for item_id in range(3):
                await self.emit({"id": item_id})

    spider = BatchSpider()
    pipeline = NativeBatchPipeline()
    engine = Engine(
        spider,
        item_pipelines=[pipeline],
        item_batch_size=3,
        item_batch_wait=0.05,
    )
    await pipeline.open(spider)
    engine._start_item_worker()
    try:
        await engine._run_callback(
            spider.produce,
            name="produce",
            url=None,
            response=None,
            parent=None,
        )
    finally:
        await engine._close_item_worker()
        await engine.http.close()

    assert pipeline.batches == [[{"id": 0}, {"id": 1}, {"id": 2}]]
    assert engine.stats.get("items_scraped") == 3


async def test_engine_batch_preserves_per_item_drop_semantics() -> None:
    class DropSecond:
        async def open(self, spider: Spider) -> None:
            pass

        async def close(self, spider: Spider) -> None:
            pass

        async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
            if item == {"id": 2}:
                raise DropItem(reason="invalid")
            return item

    class NativeSink:
        native_batch = True

        def __init__(self) -> None:
            self.items: list[JSONValue] = []

        async def open(self, spider: Spider) -> None:
            pass

        async def close(self, spider: Spider) -> None:
            pass

        async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
            return item

        async def process_items(
            self, items: list[JSONValue], spider: Spider
        ) -> list[JSONValue]:
            self.items.extend(items)
            return items

    sink = NativeSink()
    engine = Engine(Spider(), item_pipelines=[DropSecond(), sink])
    accepted = await engine._process_items([{"id": 1}, {"id": 2}, {"id": 3}])

    assert accepted == 2
    assert sink.items == [{"id": 1}, {"id": 3}]
    assert engine.stats.get("items_dropped") == 1
    assert engine.stats.labeled["items_dropped_by_reason"] == {"invalid": 1}
    await engine.http.close()


async def test_engine_batch_failure_reaches_callback() -> None:
    class FailingBatch:
        native_batch = True

        async def open(self, spider: Spider) -> None:
            pass

        async def close(self, spider: Spider) -> None:
            pass

        async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
            return item

        async def process_items(
            self, items: list[JSONValue], spider: Spider
        ) -> list[JSONValue]:
            raise RuntimeError("bulk failed")

    class BatchSpider(Spider):
        async def produce(self) -> None:
            await self.emit({"id": 1})

    spider = BatchSpider()
    engine = Engine(
        spider,
        item_pipelines=[FailingBatch()],
        item_batch_size=10,
        item_batch_wait=0.05,
    )
    engine._start_item_worker()
    try:
        with pytest.raises(SpiderError) as caught:
            await engine._run_callback(
                spider.produce,
                name="produce",
                url=None,
                response=None,
                parent=None,
            )
        assert isinstance(caught.value.__cause__, RuntimeError)
        assert engine._items_reserved == 0
    finally:
        await engine._close_item_worker()
        await engine.http.close()


async def test_json_lines_batch_uses_one_write(tmp_path) -> None:
    pipeline = JsonLinesPipeline(tmp_path / "items.jl", use_opendal=False)
    spider = Spider()
    items: list[JSONValue] = [{"id": 1}, {"id": 2}]

    await pipeline.open(spider)
    assert await pipeline.process_items(items, spider) is items
    await pipeline.close(spider)

    assert [
        json.loads(line) for line in (tmp_path / "items.jl").read_text().splitlines()
    ] == items


async def test_sqlite_batch_inserts_all_items_in_one_transaction(tmp_path) -> None:
    path = tmp_path / "items.db"
    pipeline = SQLitePipeline(path)
    spider = Spider(name="batch")
    items: list[JSONValue] = [{"id": 1}, {"id": 2}]

    await pipeline.open(spider)
    assert await pipeline.process_items(items, spider) is items
    await pipeline.close(spider)

    with sqlite3.connect(path) as conn:
        rows = conn.execute("SELECT spider, data FROM items ORDER BY id").fetchall()
    assert [(name, json.loads(data)) for name, data in rows] == [
        ("batch", {"id": 1}),
        ("batch", {"id": 2}),
    ]
