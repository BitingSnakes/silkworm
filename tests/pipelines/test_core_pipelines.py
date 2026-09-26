from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import pytest
import rxml

from silkworm.pipelines import (
    CallbackPipeline,
    CSVPipeline,
    RssPipeline,
    SQLitePipeline,
    XMLPipeline,
)
from silkworm.spiders import Spider


def _children_named(node: rxml.Node, name: str) -> list[rxml.Node]:
    return [child for child in node.children if child.name == name]


def _child(node: rxml.Node, name: str) -> rxml.Node:
    return next(child for child in node.children if child.name == name)


async def test_xml_pipeline_creates_valid_xml():
    with tempfile.TemporaryDirectory() as tmpdir:
        xml_path = Path(tmpdir) / "test.xml"
        pipeline = XMLPipeline(xml_path)
        spider = Spider()

        await pipeline.open(spider)
        await pipeline.process_item({"text": "Hello", "author": "John"}, spider)
        await pipeline.process_item({"text": "World", "author": "Jane"}, spider)
        await pipeline.close(spider)

        # Verify XML is valid
        root = rxml.read_file(str(xml_path), "items")

        assert root.name == "items"
        items = root.children
        assert len(items) == 2

        # Check first item
        assert items[0].name == "item"
        assert _child(items[0], "text").text == "Hello"
        assert _child(items[0], "author").text == "John"

        # Check second item
        assert items[1].name == "item"
        assert _child(items[1], "text").text == "World"
        assert _child(items[1], "author").text == "Jane"


async def test_xml_pipeline_custom_elements():
    with tempfile.TemporaryDirectory() as tmpdir:
        xml_path = Path(tmpdir) / "test.xml"
        pipeline = XMLPipeline(xml_path, root_element="quotes", item_element="quote")
        spider = Spider()

        await pipeline.open(spider)
        await pipeline.process_item({"text": "Test"}, spider)
        await pipeline.close(spider)

        root = rxml.read_file(str(xml_path), "quotes")

        assert root.name == "quotes"
        assert root.children[0].name == "quote"


async def test_xml_pipeline_handles_nested_dict():
    with tempfile.TemporaryDirectory() as tmpdir:
        xml_path = Path(tmpdir) / "test.xml"
        pipeline = XMLPipeline(xml_path)
        spider = Spider()

        await pipeline.open(spider)
        await pipeline.process_item(
            {"user": {"name": "Alice", "age": 30}, "active": True}, spider
        )
        await pipeline.close(spider)

        root = rxml.read_file(str(xml_path), "items")
        item = root.children[0]

        user = _child(item, "user")
        assert _child(user, "name").text == "Alice"
        assert _child(user, "age").text == "30"
        assert _child(item, "active").text == "True"


async def test_xml_pipeline_handles_list():
    with tempfile.TemporaryDirectory() as tmpdir:
        xml_path = Path(tmpdir) / "test.xml"
        pipeline = XMLPipeline(xml_path)
        spider = Spider()

        await pipeline.open(spider)
        await pipeline.process_item({"tags": ["python", "web", "scraping"]}, spider)
        await pipeline.close(spider)

        root = rxml.read_file(str(xml_path), "items")
        item = root.children[0]

        tags_elem = _child(item, "tags")
        tag_items = _children_named(tags_elem, "item")
        assert len(tag_items) == 3
        assert tag_items[0].text == "python"
        assert tag_items[1].text == "web"
        assert tag_items[2].text == "scraping"


async def test_xml_pipeline_not_opened_raises_error():
    pipeline = XMLPipeline("test.xml")
    spider = Spider()

    with pytest.raises(RuntimeError, match="XMLPipeline not opened"):
        await pipeline.process_item({"test": "data"}, spider)


async def test_rss_pipeline_respects_max_items():
    with tempfile.TemporaryDirectory() as tmpdir:
        rss_path = Path(tmpdir) / "feed.xml"
        pipeline = RssPipeline(
            rss_path,
            channel_title="Test Feed",
            channel_link="https://example.com",
            channel_description="Test description",
            max_items=2,
        )
        spider = Spider()

        await pipeline.open(spider)
        await pipeline.process_item(
            {"title": "First", "link": "https://example.com/1", "description": "One"},
            spider,
        )
        await pipeline.process_item(
            {"title": "Second", "link": "https://example.com/2", "description": "Two"},
            spider,
        )
        await pipeline.process_item(
            {"title": "Third", "link": "https://example.com/3", "description": "Three"},
            spider,
        )
        await pipeline.close(spider)

        root = rxml.read_file(str(rss_path), "rss")
        channel = _child(root, "channel")
        items = _children_named(channel, "item")
        assert len(items) == 2
        titles = [_child(item, "title").text for item in items]
        assert titles == ["Second", "Third"]


async def test_rss_pipeline_skips_items_missing_required_fields():
    with tempfile.TemporaryDirectory() as tmpdir:
        rss_path = Path(tmpdir) / "feed.xml"
        pipeline = RssPipeline(
            rss_path,
            channel_title="Test Feed",
            channel_link="https://example.com",
            channel_description="Test description",
        )
        spider = Spider()

        await pipeline.open(spider)
        await pipeline.process_item(
            {"title": "Ok", "link": "https://example.com/1", "description": "One"},
            spider,
        )
        await pipeline.process_item(
            {"title": "Missing Description", "link": "https://example.com/2"},
            spider,
        )
        await pipeline.close(spider)

        root = rxml.read_file(str(rss_path), "rss")
        channel = _child(root, "channel")
        items = _children_named(channel, "item")
        assert len(items) == 1
        assert _child(items[0], "title").text == "Ok"


# CallbackPipeline tests
async def test_callback_pipeline_with_sync_callback():
    processed_items = []

    def process_item(item, spider):
        processed_items.append(item)
        return item

    pipeline = CallbackPipeline(callback=process_item)
    spider = Spider()

    await pipeline.open(spider)
    result = await pipeline.process_item({"text": "Hello", "author": "Alice"}, spider)
    await pipeline.close(spider)

    assert len(processed_items) == 1
    assert processed_items[0] == {"text": "Hello", "author": "Alice"}
    assert result == {"text": "Hello", "author": "Alice"}


async def test_callback_pipeline_with_async_callback():
    processed_items = []

    async def process_item(item, spider):
        await asyncio.sleep(0.01)  # Simulate async operation
        processed_items.append(item)
        return item

    pipeline = CallbackPipeline(callback=process_item)
    spider = Spider()

    await pipeline.open(spider)
    result = await pipeline.process_item({"text": "World", "author": "Bob"}, spider)
    await pipeline.close(spider)

    assert len(processed_items) == 1
    assert processed_items[0] == {"text": "World", "author": "Bob"}
    assert result == {"text": "World", "author": "Bob"}


async def test_callback_pipeline_callback_can_modify_item():
    def add_timestamp(item, spider):
        item["processed"] = True
        item["spider_name"] = spider.name
        return item

    pipeline = CallbackPipeline(callback=add_timestamp)
    spider = Spider()
    spider.name = "test_spider"

    await pipeline.open(spider)
    result = await pipeline.process_item({"text": "Test"}, spider)
    await pipeline.close(spider)

    assert result == {"text": "Test", "processed": True, "spider_name": "test_spider"}


async def test_callback_pipeline_callback_returning_none():
    def process_item(item, spider):
        # Callback that doesn't return anything
        print(item)

    pipeline = CallbackPipeline(callback=process_item)
    spider = Spider()

    await pipeline.open(spider)
    result = await pipeline.process_item({"text": "Test"}, spider)
    await pipeline.close(spider)

    # When callback returns None, original item should be returned
    assert result == {"text": "Test"}


async def test_callback_pipeline_multiple_items():
    processed_items = []

    def process_item(item, spider):
        processed_items.append(item)
        return item

    pipeline = CallbackPipeline(callback=process_item)
    spider = Spider()

    await pipeline.open(spider)
    await pipeline.process_item({"id": 1}, spider)
    await pipeline.process_item({"id": 2}, spider)
    await pipeline.process_item({"id": 3}, spider)
    await pipeline.close(spider)

    assert len(processed_items) == 3
    assert processed_items[0] == {"id": 1}
    assert processed_items[1] == {"id": 2}
    assert processed_items[2] == {"id": 3}


def test_callback_pipeline_requires_callable():
    with pytest.raises(TypeError, match="callback must be callable"):
        CallbackPipeline(callback="not_callable")  # pyright: ignore[reportArgumentType]

    with pytest.raises(TypeError, match="callback must be callable"):
        CallbackPipeline(callback=123)  # pyright: ignore[reportArgumentType]

    with pytest.raises(TypeError, match="callback must be callable"):
        CallbackPipeline(callback=None)  # pyright: ignore[reportArgumentType]


async def test_callback_pipeline_with_lambda():
    pipeline = CallbackPipeline(
        callback=lambda item, spider: (
            {**item, "processed": True} if isinstance(item, dict) else item
        )
    )
    spider = Spider()

    await pipeline.open(spider)
    result = await pipeline.process_item({"text": "Test"}, spider)
    await pipeline.close(spider)

    assert result == {"text": "Test", "processed": True}


async def test_callback_pipeline_callback_can_filter_item():
    """Test that callback can return a different item or filter it."""

    def filter_short_text(item, spider):
        if len(item.get("text", "")) < 5:
            return None  # Filter out short items
        return item

    pipeline = CallbackPipeline(callback=filter_short_text)
    spider = Spider()

    await pipeline.open(spider)

    # Short text should still return original item when callback returns None
    result1 = await pipeline.process_item({"text": "Hi"}, spider)
    assert result1 == {"text": "Hi"}

    # Long text should pass through
    result2 = await pipeline.process_item({"text": "Hello World"}, spider)
    assert result2 == {"text": "Hello World"}

    await pipeline.close(spider)


async def test_csv_pipeline_creates_valid_csv():
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = Path(tmpdir) / "test.csv"
        pipeline = CSVPipeline(csv_path)
        spider = Spider()

        await pipeline.open(spider)
        await pipeline.process_item({"text": "Hello", "author": "John"}, spider)
        await pipeline.process_item({"text": "World", "author": "Jane"}, spider)
        await pipeline.close(spider)

        # Read and verify CSV
        content = csv_path.read_text()
        lines = content.strip().split("\n")

        assert len(lines) == 3  # header + 2 data rows
        # Check header has both fields (order may vary)
        header_fields = set(lines[0].split(","))
        assert header_fields == {"text", "author"}
        assert "Hello" in content
        assert "John" in content
        assert "World" in content
        assert "Jane" in content


async def test_csv_pipeline_with_custom_fieldnames():
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = Path(tmpdir) / "test.csv"
        pipeline = CSVPipeline(csv_path, fieldnames=["author", "text"])
        spider = Spider()

        await pipeline.open(spider)
        await pipeline.process_item({"text": "Hello", "author": "John"}, spider)
        await pipeline.close(spider)

        content = csv_path.read_text()
        lines = content.strip().split("\n")

        # Verify header order matches custom fieldnames
        assert lines[0] == "author,text"
        assert "John,Hello" in content


async def test_csv_pipeline_flattens_nested_dict():
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = Path(tmpdir) / "test.csv"
        pipeline = CSVPipeline(csv_path)
        spider = Spider()

        await pipeline.open(spider)
        await pipeline.process_item(
            {"name": "Alice", "address": {"city": "NYC", "zip": "10001"}}, spider
        )
        await pipeline.close(spider)

        content = csv_path.read_text()

        assert "address_city" in content
        assert "address_zip" in content
        assert "NYC" in content
        assert "10001" in content


async def test_csv_pipeline_converts_list_to_string():
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = Path(tmpdir) / "test.csv"
        pipeline = CSVPipeline(csv_path)
        spider = Spider()

        await pipeline.open(spider)
        await pipeline.process_item(
            {"author": "John", "tags": ["python", "web", "scraping"]}, spider
        )
        await pipeline.close(spider)

        content = csv_path.read_text()

        assert "python, web, scraping" in content


async def test_csv_pipeline_not_opened_raises_error():
    pipeline = CSVPipeline("test.csv")
    spider = Spider()

    with pytest.raises(RuntimeError, match="CSVPipeline not opened"):
        await pipeline.process_item({"test": "data"}, spider)


async def test_csv_pipeline_handles_extra_fields():
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = Path(tmpdir) / "test.csv"
        pipeline = CSVPipeline(csv_path, fieldnames=["author"])
        spider = Spider()

        await pipeline.open(spider)
        # Item has extra field "text" that's not in fieldnames
        await pipeline.process_item({"text": "Hello", "author": "John"}, spider)
        await pipeline.close(spider)

        content = csv_path.read_text()
        lines = content.strip().split("\n")

        # Only author should be in output
        assert lines[0] == "author"
        assert "John" in content
        # text should not be in output
        assert "Hello" not in content


def test_sqlite_pipeline_invalid_table_name():
    # Test that invalid table names are rejected
    with pytest.raises(ValueError, match="Invalid table name"):
        SQLitePipeline(table="invalid-table-name")

    with pytest.raises(ValueError, match="Invalid table name"):
        SQLitePipeline(table="123invalid")

    with pytest.raises(ValueError, match="Invalid table name"):
        SQLitePipeline(table="table; DROP TABLE users;")
