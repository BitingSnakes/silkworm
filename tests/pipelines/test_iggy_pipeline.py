from __future__ import annotations

import json
from datetime import timedelta
from typing import Any, ClassVar, cast

import pytest

from silkworm.spiders import Spider


class _FakeMessage:
    def __init__(self, data: str) -> None:
        self.data = data


class _FakePartitioning:
    balanced_value: ClassVar[object] = object()

    @classmethod
    def balanced(cls) -> object:
        return cls.balanced_value


class _FakeProducer:
    def __init__(self) -> None:
        self.messages: list[_FakeMessage] = []
        self.shutdown_calls = 0

    async def send_one(self, message: _FakeMessage) -> None:
        self.messages.append(message)

    async def shutdown(self) -> None:
        self.shutdown_calls += 1


class _FakeClient:
    created: ClassVar[list[_FakeClient]] = []

    def __init__(self, connection_string: str = "injected") -> None:
        self.connection_string = connection_string
        self.connected = False
        self.producer_calls: list[tuple[str, str, dict[str, object]]] = []
        self.producer_instance = _FakeProducer()

    @classmethod
    def from_connection_string(cls, connection_string: str) -> _FakeClient:
        client = cls(connection_string)
        cls.created.append(client)
        return client

    async def connect(self) -> None:
        self.connected = True

    async def producer(
        self,
        stream: str,
        topic: str,
        **options: object,
    ) -> _FakeProducer:
        self.producer_calls.append((stream, topic, options))
        return self.producer_instance


@pytest.fixture
def fake_iggy(monkeypatch: pytest.MonkeyPatch) -> Any:
    from silkworm._pipelines import iggy_pipeline

    _FakeClient.created = []
    _FakePartitioning.balanced_value = object()
    monkeypatch.setattr(iggy_pipeline, "IggyClient", _FakeClient)
    monkeypatch.setattr(iggy_pipeline, "Partitioning", _FakePartitioning)
    monkeypatch.setattr(iggy_pipeline, "SendMessage", _FakeMessage)
    monkeypatch.setattr(iggy_pipeline, "IGGY_AVAILABLE", True)
    return iggy_pipeline


async def test_iggy_pipeline_publishes_compact_utf8_json(fake_iggy: Any) -> None:
    pipeline = fake_iggy.IggyPipeline(
        "scraping",
        "items",
        connection_string="iggy+tcp://user:secret@iggy.example:8090",
    )
    spider = Spider(name="quotes")
    item = {"text": "Привіт", "rank": 1}

    await pipeline.open(spider)
    returned = await pipeline.process_item(item, spider)
    await pipeline.close(spider)

    client = _FakeClient.created[0]
    assert returned is item
    assert client.connected
    assert client.connection_string == "iggy+tcp://user:secret@iggy.example:8090"
    assert json.loads(client.producer_instance.messages[0].data) == item
    assert "Привіт" in client.producer_instance.messages[0].data
    assert " " not in client.producer_instance.messages[0].data
    assert client.producer_instance.shutdown_calls == 1


async def test_iggy_pipeline_configures_high_level_producer(fake_iggy: Any) -> None:
    client = _FakeClient()
    partitioning = object()
    mode = object()
    retry_interval = timedelta(milliseconds=250)
    pipeline = fake_iggy.IggyPipeline(
        "events",
        "products",
        client=cast("Any", client),
        partitioning=cast("Any", partitioning),
        mode=cast("Any", mode),
        create_stream_if_not_exists=False,
        create_topic_if_not_exists=False,
        topic_partitions_count=4,
        send_retries=7,
        send_retry_interval=retry_interval,
    )

    await pipeline.open(Spider())

    assert not client.connected
    assert client.producer_calls == [
        (
            "events",
            "products",
            {
                "partitioning": partitioning,
                "mode": mode,
                "create_stream_if_not_exists": False,
                "create_topic_if_not_exists": False,
                "topic_partitions_count": 4,
                "send_retries": 7,
                "send_retry_interval": retry_interval,
            },
        )
    ]

    await pipeline.close(Spider())


async def test_iggy_pipeline_uses_balanced_partitioning_by_default(
    fake_iggy: Any,
) -> None:
    pipeline = fake_iggy.IggyPipeline("scraping", "items")

    await pipeline.open(Spider())

    client = _FakeClient.created[0]
    _, _, options = client.producer_calls[0]
    assert options["partitioning"] is _FakePartitioning.balanced_value

    await pipeline.close(Spider())


async def test_iggy_pipeline_lifecycle_guards(fake_iggy: Any) -> None:
    pipeline = fake_iggy.IggyPipeline("scraping", "items")
    spider = Spider()

    with pytest.raises(RuntimeError, match="IggyPipeline not opened"):
        await pipeline.process_item({"id": 1}, spider)

    await pipeline.open(spider)
    with pytest.raises(RuntimeError, match="IggyPipeline already opened"):
        await pipeline.open(spider)
    await pipeline.close(spider)


def test_iggy_pipeline_rejects_connection_string_with_client(fake_iggy: Any) -> None:
    with pytest.raises(ValueError, match="mutually exclusive"):
        fake_iggy.IggyPipeline(
            "scraping",
            "items",
            connection_string="iggy+tcp://iggy:iggy@localhost:8090",
            client=cast("Any", _FakeClient()),
        )


def test_iggy_pipeline_requires_optional_dependency(
    fake_iggy: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fake_iggy, "IGGY_AVAILABLE", False)

    with pytest.raises(ImportError, match=r"silkworm-rs\[iggy\]"):
        fake_iggy.IggyPipeline("scraping", "items")
