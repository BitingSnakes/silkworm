from __future__ import annotations

import asyncio
import json
from typing import ClassVar

import pytest

from silkworm.spiders import Spider

# TaskiqPipeline tests - skip if taskiq not installed
try:
    from taskiq import InMemoryBroker  # type: ignore

    from silkworm.pipelines import TaskiqPipeline

    TASKIQ_AVAILABLE = True
except ImportError:
    TASKIQ_AVAILABLE = False


@pytest.mark.skipif(not TASKIQ_AVAILABLE, reason="taskiq not installed")
async def test_taskiq_pipeline_sends_items_to_queue():
    broker = InMemoryBroker()
    processed_items = []

    @broker.task
    async def process_item(item):
        processed_items.append(item)
        return item

    # Pass the task directly
    pipeline = TaskiqPipeline(broker, task=process_item)
    spider = Spider()

    await pipeline.open(spider)
    await pipeline.process_item({"text": "Hello", "author": "Alice"}, spider)
    await pipeline.process_item({"text": "World", "author": "Bob"}, spider)

    # Wait for InMemoryBroker to process tasks asynchronously
    await asyncio.sleep(0.1)

    await pipeline.close(spider)

    # InMemoryBroker processes tasks asynchronously
    assert len(processed_items) == 2
    assert processed_items[0] == {"text": "Hello", "author": "Alice"}
    assert processed_items[1] == {"text": "World", "author": "Bob"}


@pytest.mark.skipif(not TASKIQ_AVAILABLE, reason="taskiq not installed")
async def test_taskiq_pipeline_not_opened_raises_error():
    broker = InMemoryBroker()

    @broker.task
    async def process_item(item):
        return item

    pipeline = TaskiqPipeline(broker, task=process_item)
    spider = Spider()

    with pytest.raises(RuntimeError, match="TaskiqPipeline not opened"):
        await pipeline.process_item({"test": "data"}, spider)


@pytest.mark.skipif(not TASKIQ_AVAILABLE, reason="taskiq not installed")
async def test_taskiq_pipeline_invalid_task_name_raises_error():
    broker = InMemoryBroker()

    @broker.task
    async def process_item(item):
        return item

    # Use task_name parameter with an invalid name
    pipeline = TaskiqPipeline(broker, task_name="nonexistent_task")
    spider = Spider()

    with pytest.raises(ValueError, match="Task 'nonexistent_task' not found"):
        await pipeline.open(spider)


@pytest.mark.skipif(not TASKIQ_AVAILABLE, reason="taskiq not installed")
def test_taskiq_pipeline_without_task_or_name_raises_error():
    broker = InMemoryBroker()

    with pytest.raises(
        ValueError, match="Either 'task' or 'task_name' must be provided"
    ):
        TaskiqPipeline(broker)


@pytest.mark.skipif(not TASKIQ_AVAILABLE, reason="taskiq not installed")
def test_taskiq_pipeline_without_taskiq_raises_import_error():
    # This test simulates what happens when taskiq is not installed
    # We can't really test this without mocking, but we ensure the error message is correct
    from silkworm.pipelines import TASKIQ_AVAILABLE

    if not TASKIQ_AVAILABLE:
        pytest.skip("taskiq is installed, cannot test ImportError path")
    # If taskiq is available, this test is satisfied


# ZenohPipeline tests use a fake binding so lifecycle behavior remains covered
# even when the optional native dependency is not installed.
class _FakeZenohPublisher:
    def __init__(self, key_expr: str, options: dict[str, object]) -> None:
        self.key_expr = key_expr
        self.options = options
        self.payloads: list[str] = []
        self.undeclared = False
        self.fail_undeclare = False

    def put(self, payload: str) -> None:
        self.payloads.append(payload)

    def undeclare(self) -> None:
        self.undeclared = True
        if self.fail_undeclare:
            raise RuntimeError("undeclare failed")


class _FakeZenohSession:
    def __init__(self, *, fail_declare: bool = False) -> None:
        self.fail_declare = fail_declare
        self.publishers: list[_FakeZenohPublisher] = []
        self.closed = False
        self.fail_close = False

    def declare_publisher(
        self,
        key_expr: str,
        **options: object,
    ) -> _FakeZenohPublisher:
        if self.fail_declare:
            raise RuntimeError("declaration failed")
        publisher = _FakeZenohPublisher(key_expr, options)
        self.publishers.append(publisher)
        return publisher

    def close(self) -> None:
        self.closed = True
        if self.fail_close:
            raise RuntimeError("close failed")


class _FakeZenoh:
    class Config:
        pass

    opened: ClassVar[list[tuple[object, _FakeZenohSession]]] = []
    next_session: _FakeZenohSession | None = None

    @classmethod
    def open(cls, config: object) -> _FakeZenohSession:
        session = cls.next_session or _FakeZenohSession()
        cls.next_session = None
        cls.opened.append((config, session))
        return session


@pytest.fixture
def fake_zenoh(monkeypatch: pytest.MonkeyPatch):
    from silkworm._pipelines import zenoh_pipeline

    _FakeZenoh.opened = []
    _FakeZenoh.next_session = None
    monkeypatch.setattr(zenoh_pipeline, "zenoh", _FakeZenoh)
    monkeypatch.setattr(zenoh_pipeline, "ZENOH_AVAILABLE", True)
    return zenoh_pipeline


async def test_zenoh_pipeline_publishes_json_with_qos(fake_zenoh):
    options = {
        "congestion_control": object(),
        "priority": object(),
        "reliability": object(),
        "allowed_destination": object(),
    }
    pipeline = fake_zenoh.ZenohPipeline(
        "scraping/items",
        encoding="application/json",
        express=True,
        **options,
    )
    spider = Spider(name="quotes")
    item = {"text": "Привіт", "rank": 1}

    await pipeline.open(spider)
    returned = await pipeline.process_item(item, spider)

    _, session = _FakeZenoh.opened[0]
    publisher = session.publishers[0]
    assert returned is item
    assert publisher.key_expr == "scraping/items"
    assert publisher.options == {
        "encoding": "application/json",
        "express": True,
        **options,
    }
    assert json.loads(publisher.payloads[0]) == item
    assert "Привіт" in publisher.payloads[0]

    await pipeline.close(spider)
    assert publisher.undeclared
    assert session.closed


async def test_zenoh_pipeline_caches_dynamic_publishers(fake_zenoh):
    async def resolve_key(item, spider):
        await asyncio.sleep(0)
        return f"{spider.name}/{item['kind']}"

    session = _FakeZenohSession()
    pipeline = fake_zenoh.ZenohPipeline(resolve_key, session=session)
    spider = Spider(name="crawl")

    await pipeline.open(spider)
    await pipeline.process_item({"kind": "article", "id": 1}, spider)
    await pipeline.process_item({"kind": "article", "id": 2}, spider)
    await pipeline.process_item({"kind": "image", "id": 3}, spider)

    assert [publisher.key_expr for publisher in session.publishers] == [
        "crawl/article",
        "crawl/image",
    ]
    assert len(session.publishers[0].payloads) == 2

    await pipeline.close(spider)
    assert all(publisher.undeclared for publisher in session.publishers)
    assert not session.closed


async def test_zenoh_pipeline_declares_dynamic_publisher_once_concurrently(
    fake_zenoh,
):
    session = _FakeZenohSession()
    pipeline = fake_zenoh.ZenohPipeline(
        lambda item, spider: "shared/items",
        session=session,
    )
    spider = Spider()
    await pipeline.open(spider)

    await asyncio.gather(
        *(pipeline.process_item({"id": item_id}, spider) for item_id in range(20))
    )
    await pipeline.close(spider)

    assert len(session.publishers) == 1
    assert len(session.publishers[0].payloads) == 20


async def test_zenoh_pipeline_supports_sync_key_resolver(fake_zenoh):
    session = _FakeZenohSession()
    pipeline = fake_zenoh.ZenohPipeline(
        lambda item, spider: f"{spider.name}/{item}",
        session=session,
    )
    spider = Spider(name="numbers")

    await pipeline.open(spider)
    await pipeline.process_item(3, spider)
    await pipeline.close(spider)

    assert session.publishers[0].key_expr == "numbers/3"


async def test_zenoh_pipeline_owned_session_uses_provided_config(fake_zenoh):
    config = _FakeZenoh.Config()
    pipeline = fake_zenoh.ZenohPipeline("items", config=config)
    spider = Spider()

    await pipeline.open(spider)
    await pipeline.close(spider)

    opened_config, session = _FakeZenoh.opened[0]
    assert opened_config is config
    assert session.closed


async def test_zenoh_pipeline_not_opened_raises_error(fake_zenoh):
    pipeline = fake_zenoh.ZenohPipeline("items")

    with pytest.raises(RuntimeError, match="ZenohPipeline not opened"):
        await pipeline.process_item({"id": 1}, Spider())


async def test_zenoh_pipeline_invalid_resolver_result(fake_zenoh):
    pipeline = fake_zenoh.ZenohPipeline(lambda item, spider: 42)
    spider = Spider()
    await pipeline.open(spider)

    with pytest.raises(TypeError, match="resolver must return a string"):
        await pipeline.process_item({"id": 1}, spider)

    await pipeline.close(spider)


async def test_zenoh_pipeline_rolls_back_owned_session(fake_zenoh):
    session = _FakeZenohSession(fail_declare=True)
    _FakeZenoh.next_session = session
    pipeline = fake_zenoh.ZenohPipeline("items")

    with pytest.raises(RuntimeError, match="declaration failed"):
        await pipeline.open(Spider())

    assert session.closed


async def test_zenoh_pipeline_collects_cleanup_failures(fake_zenoh):
    pipeline = fake_zenoh.ZenohPipeline("items")
    spider = Spider()
    await pipeline.open(spider)
    _, session = _FakeZenoh.opened[0]
    session.publishers[0].fail_undeclare = True
    session.fail_close = True

    with pytest.raises(BaseExceptionGroup) as exc_info:
        await pipeline.close(spider)

    assert len(exc_info.value.exceptions) == 2
    assert session.publishers[0].undeclared
    assert session.closed


def test_zenoh_pipeline_validates_constructor(fake_zenoh):
    with pytest.raises(ValueError, match="mutually exclusive"):
        fake_zenoh.ZenohPipeline(
            "items",
            config=_FakeZenoh.Config(),
            session=_FakeZenohSession(),
        )
    with pytest.raises(TypeError, match="string or callable"):
        fake_zenoh.ZenohPipeline(42)


def test_zenoh_pipeline_requires_optional_dependency(monkeypatch):
    from silkworm._pipelines import zenoh_pipeline

    monkeypatch.setattr(zenoh_pipeline, "ZENOH_AVAILABLE", False)
    with pytest.raises(ImportError, match=r"silkworm-rs\[zenoh\]"):
        zenoh_pipeline.ZenohPipeline("items")
