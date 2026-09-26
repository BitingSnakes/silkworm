from __future__ import annotations

import io
import json
import sys
import tempfile
from pathlib import Path
from typing import Any, cast

import anyio
import pytest

from silkworm.spiders import Spider

# MsgPackPipeline tests - skip if ormsgpack not installed
# Note: We import both ormsgpack and msgpack. ormsgpack is required for the
# MsgPackPipeline to work (for writing), but we use msgpack for reading in tests
# because ormsgpack doesn't have an Unpacker class to read multiple objects from a stream.
try:
    import msgpack  # type: ignore
    import ormsgpack  # type: ignore  # noqa: F401

    from silkworm.pipelines import MsgPackPipeline

    ORMSGPACK_AVAILABLE = True
except ImportError:
    ORMSGPACK_AVAILABLE = False


@pytest.mark.skipif(not ORMSGPACK_AVAILABLE, reason="ormsgpack not installed")
async def test_msgpack_pipeline_writes_items():
    with tempfile.TemporaryDirectory() as tmpdir:
        msgpack_path = Path(tmpdir) / "test.msgpack"
        pipeline = MsgPackPipeline(msgpack_path)
        spider = Spider()

        await pipeline.open(spider)
        await pipeline.process_item({"text": "Hello", "author": "John"}, spider)
        await pipeline.process_item({"text": "World", "author": "Jane"}, spider)
        await pipeline.close(spider)

        # Read and verify MsgPack data
        f = io.BytesIO(await anyio.Path(msgpack_path).read_bytes())
        data = f.read()

        # Unpack both items using msgpack.Unpacker
        unpacker = msgpack.Unpacker()
        unpacker.feed(data)
        items = list(unpacker)

        assert len(items) == 2
        assert items[0] == {"text": "Hello", "author": "John"}
        assert items[1] == {"text": "World", "author": "Jane"}


@pytest.mark.skipif(not ORMSGPACK_AVAILABLE, reason="ormsgpack not installed")
async def test_msgpack_pipeline_append_mode():
    with tempfile.TemporaryDirectory() as tmpdir:
        msgpack_path = Path(tmpdir) / "test.msgpack"
        spider = Spider()

        # Write first item
        pipeline1 = MsgPackPipeline(msgpack_path, mode="write")
        await pipeline1.open(spider)
        await pipeline1.process_item({"text": "First"}, spider)
        await pipeline1.close(spider)

        # Append second item
        pipeline2 = MsgPackPipeline(msgpack_path, mode="append")
        await pipeline2.open(spider)
        await pipeline2.process_item({"text": "Second"}, spider)
        await pipeline2.close(spider)

        # Read and verify both items
        f = io.BytesIO(await anyio.Path(msgpack_path).read_bytes())
        data = f.read()

        unpacker = msgpack.Unpacker()
        unpacker.feed(data)
        items = list(unpacker)

        assert len(items) == 2
        assert items[0] == {"text": "First"}
        assert items[1] == {"text": "Second"}


@pytest.mark.skipif(not ORMSGPACK_AVAILABLE, reason="ormsgpack not installed")
async def test_msgpack_pipeline_handles_nested_data():
    with tempfile.TemporaryDirectory() as tmpdir:
        msgpack_path = Path(tmpdir) / "test.msgpack"
        pipeline = MsgPackPipeline(msgpack_path)
        spider = Spider()

        await pipeline.open(spider)
        await pipeline.process_item(
            {"user": {"name": "Alice", "age": 30}, "tags": ["python", "web"]}, spider
        )
        await pipeline.close(spider)

        # Read and verify
        f = io.BytesIO(await anyio.Path(msgpack_path).read_bytes())
        data = f.read()

        unpacker = msgpack.Unpacker()
        unpacker.feed(data)
        items = list(unpacker)

        assert len(items) == 1
        assert items[0] == {
            "user": {"name": "Alice", "age": 30},
            "tags": ["python", "web"],
        }


@pytest.mark.skipif(not ORMSGPACK_AVAILABLE, reason="ormsgpack not installed")
async def test_msgpack_pipeline_not_opened_raises_error():
    pipeline = MsgPackPipeline("test.msgpack")
    spider = Spider()

    with pytest.raises(RuntimeError, match="MsgPackPipeline not opened"):
        await pipeline.process_item({"test": "data"}, spider)


@pytest.mark.skipif(not ORMSGPACK_AVAILABLE, reason="ormsgpack not installed")
def test_msgpack_pipeline_invalid_mode_raises_error():
    with pytest.raises(ValueError, match="mode must be 'write' or 'append'"):
        MsgPackPipeline("test.msgpack", mode="invalid")


# PolarsPipeline tests - skip if polars not installed
try:
    import polars as pl  # type: ignore

    from silkworm.pipelines import PolarsPipeline

    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False


@pytest.mark.skipif(not POLARS_AVAILABLE, reason="polars not installed")
async def test_polars_pipeline_writes_parquet():
    with tempfile.TemporaryDirectory() as tmpdir:
        parquet_path = Path(tmpdir) / "test.parquet"
        pipeline = PolarsPipeline(parquet_path)
        spider = Spider()

        await pipeline.open(spider)
        await pipeline.process_item({"text": "Hello", "author": "John"}, spider)
        await pipeline.process_item({"text": "World", "author": "Jane"}, spider)
        await pipeline.close(spider)

        # Read and verify Parquet data
        df = pl.read_parquet(parquet_path)
        assert len(df) == 2
        assert df["text"].to_list() == ["Hello", "World"]
        assert df["author"].to_list() == ["John", "Jane"]


@pytest.mark.skipif(not POLARS_AVAILABLE, reason="polars not installed")
async def test_polars_pipeline_append_mode():
    with tempfile.TemporaryDirectory() as tmpdir:
        parquet_path = Path(tmpdir) / "test.parquet"
        spider = Spider()

        # Write first item
        pipeline1 = PolarsPipeline(parquet_path, mode="write")
        await pipeline1.open(spider)
        await pipeline1.process_item({"text": "First"}, spider)
        await pipeline1.close(spider)

        # Append second item
        pipeline2 = PolarsPipeline(parquet_path, mode="append")
        await pipeline2.open(spider)
        await pipeline2.process_item({"text": "Second"}, spider)
        await pipeline2.close(spider)

        # Read and verify both items
        df = pl.read_parquet(parquet_path)
        assert len(df) == 2
        assert df["text"].to_list() == ["First", "Second"]


@pytest.mark.skipif(not POLARS_AVAILABLE, reason="polars not installed")
def test_polars_pipeline_invalid_mode_raises_error():
    with pytest.raises(ValueError, match="mode must be 'write' or 'append'"):
        PolarsPipeline("test.parquet", mode="invalid")


# ExcelPipeline tests - skip if openpyxl not installed
try:
    import openpyxl  # type: ignore

    from silkworm.pipelines import ExcelPipeline

    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False


@pytest.mark.skipif(not OPENPYXL_AVAILABLE, reason="openpyxl not installed")
async def test_excel_pipeline_writes_xlsx():
    with tempfile.TemporaryDirectory() as tmpdir:
        excel_path = Path(tmpdir) / "test.xlsx"
        pipeline = ExcelPipeline(excel_path, sheet_name="quotes")
        spider = Spider()

        await pipeline.open(spider)
        await pipeline.process_item({"text": "Hello", "author": "John"}, spider)
        await pipeline.process_item({"text": "World", "author": "Jane"}, spider)
        await pipeline.close(spider)

        # Read and verify Excel data
        wb = openpyxl.load_workbook(excel_path)
        ws = wb["quotes"]

        # Check header
        header = [cell.value for cell in ws[1]]
        assert set(header) == {"text", "author"}

        # Check data
        rows = list(ws.iter_rows(min_row=2, values_only=True))
        assert len(rows) == 2
        assert any(
            row[header.index("text")] == "Hello"
            and row[header.index("author")] == "John"
            for row in rows
        )
        assert any(
            row[header.index("text")] == "World"
            and row[header.index("author")] == "Jane"
            for row in rows
        )


@pytest.mark.skipif(not OPENPYXL_AVAILABLE, reason="openpyxl not installed")
async def test_excel_pipeline_flattens_nested_dict():
    with tempfile.TemporaryDirectory() as tmpdir:
        excel_path = Path(tmpdir) / "test.xlsx"
        pipeline = ExcelPipeline(excel_path)
        spider = Spider()

        await pipeline.open(spider)
        await pipeline.process_item(
            {"name": "Alice", "address": {"city": "NYC", "zip": "10001"}}, spider
        )
        await pipeline.close(spider)

        # Read and verify
        wb = openpyxl.load_workbook(excel_path)
        ws = wb.active
        header = [cell.value for cell in ws[1]]

        assert "address_city" in header
        assert "address_zip" in header


# YAMLPipeline tests - skip if pyyaml not installed
try:
    import yaml  # type: ignore

    from silkworm.pipelines import YAMLPipeline

    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False


@pytest.mark.skipif(not YAML_AVAILABLE, reason="pyyaml not installed")
async def test_yaml_pipeline_writes_yaml():
    with tempfile.TemporaryDirectory() as tmpdir:
        yaml_path = Path(tmpdir) / "test.yaml"
        pipeline = YAMLPipeline(yaml_path)
        spider = Spider()

        await pipeline.open(spider)
        await pipeline.process_item({"text": "Hello", "author": "John"}, spider)
        await pipeline.process_item({"text": "World", "author": "Jane"}, spider)
        await pipeline.close(spider)

        # Read and verify YAML data
        f = io.StringIO((await anyio.Path(yaml_path).read_bytes()).decode("utf-8"))
        data = yaml.safe_load(f)

        assert len(data) == 2
        assert data[0] == {"text": "Hello", "author": "John"}
        assert data[1] == {"text": "World", "author": "Jane"}


@pytest.mark.skipif(not YAML_AVAILABLE, reason="pyyaml not installed")
async def test_yaml_pipeline_handles_nested_data():
    with tempfile.TemporaryDirectory() as tmpdir:
        yaml_path = Path(tmpdir) / "test.yaml"
        pipeline = YAMLPipeline(yaml_path)
        spider = Spider()

        await pipeline.open(spider)
        await pipeline.process_item(
            {"user": {"name": "Alice", "age": 30}, "tags": ["python", "web"]}, spider
        )
        await pipeline.close(spider)

        # Read and verify
        f = io.StringIO((await anyio.Path(yaml_path).read_bytes()).decode("utf-8"))
        data = yaml.safe_load(f)

        assert len(data) == 1
        assert data[0] == {
            "user": {"name": "Alice", "age": 30},
            "tags": ["python", "web"],
        }


# AvroPipeline tests - skip if fastavro not installed
try:
    import fastavro  # type: ignore

    from silkworm.pipelines import AvroPipeline

    FASTAVRO_AVAILABLE = True
except ImportError:
    FASTAVRO_AVAILABLE = False


@pytest.mark.skipif(not FASTAVRO_AVAILABLE, reason="fastavro not installed")
async def test_avro_pipeline_writes_with_schema():
    with tempfile.TemporaryDirectory() as tmpdir:
        avro_path = Path(tmpdir) / "test.avro"
        schema = {
            "type": "record",
            "name": "Quote",
            "fields": [
                {"name": "text", "type": "string"},
                {"name": "author", "type": "string"},
            ],
        }
        pipeline = AvroPipeline(avro_path, schema=schema)
        spider = Spider()

        await pipeline.open(spider)
        await pipeline.process_item({"text": "Hello", "author": "John"}, spider)
        await pipeline.process_item({"text": "World", "author": "Jane"}, spider)
        await pipeline.close(spider)

        # Read and verify Avro data
        f = io.BytesIO(await anyio.Path(avro_path).read_bytes())
        reader = fastavro.reader(f)
        records = list(reader)

        assert len(records) == 2
        assert records[0] == {"text": "Hello", "author": "John"}
        assert records[1] == {"text": "World", "author": "Jane"}


@pytest.mark.skipif(not FASTAVRO_AVAILABLE, reason="fastavro not installed")
async def test_avro_pipeline_infers_schema():
    with tempfile.TemporaryDirectory() as tmpdir:
        avro_path = Path(tmpdir) / "test.avro"
        pipeline = AvroPipeline(avro_path)  # No schema provided
        spider = Spider()

        await pipeline.open(spider)
        await pipeline.process_item(
            {"text": "Hello", "author": "John", "count": 5}, spider
        )
        await pipeline.close(spider)

        # Read and verify Avro data
        f = io.BytesIO(await anyio.Path(avro_path).read_bytes())
        reader = fastavro.reader(f)
        records = [cast("dict[str, Any]", record) for record in reader]

        assert len(records) == 1
        assert records[0]["text"] == "Hello"
        assert records[0]["author"] == "John"
        assert records[0]["count"] == 5


# ElasticsearchPipeline tests - skip if elasticsearch not installed
try:
    from elasticsearch import (  # pyright: ignore[reportMissingImports]
        AsyncElasticsearch,  # noqa: F401
    )

    from silkworm.pipelines import ElasticsearchPipeline

    ELASTICSEARCH_AVAILABLE = True
except ImportError:
    ELASTICSEARCH_AVAILABLE = False
    ElasticsearchPipeline = None


@pytest.mark.skipif(not ELASTICSEARCH_AVAILABLE, reason="elasticsearch not installed")
def test_elasticsearch_pipeline_initialization():
    # Just test that we can initialize the pipeline
    pipeline = ElasticsearchPipeline(  # type: ignore
        hosts=["http://localhost:9200"],
        index="test_index",
    )
    assert pipeline.index == "test_index"
    assert pipeline.hosts == ["http://localhost:9200"]


# MongoDBPipeline tests - skip if motor not installed
try:
    import motor.motor_asyncio  # type: ignore[import-not-found]  # noqa: F401

    from silkworm.pipelines import MongoDBPipeline

    MOTOR_AVAILABLE = True
except ImportError:
    MOTOR_AVAILABLE = False
    MongoDBPipeline = None


@pytest.mark.skipif(not MOTOR_AVAILABLE, reason="motor not installed")
def test_mongodb_pipeline_initialization():
    # Just test that we can initialize the pipeline
    pipeline = MongoDBPipeline(  # type: ignore
        connection_string="mongodb://localhost:27017",
        database="test_db",
        collection="test_collection",
    )
    assert pipeline.database == "test_db"
    assert pipeline.collection == "test_collection"


# S3JsonLinesPipeline tests - skip if opendal not installed
try:
    import opendal  # type: ignore[import-not-found]  # noqa: F401

    from silkworm.pipelines import S3JsonLinesPipeline

    OPENDAL_AVAILABLE = True
except ImportError:
    OPENDAL_AVAILABLE = False
    S3JsonLinesPipeline = None


@pytest.mark.skipif(not OPENDAL_AVAILABLE, reason="opendal not installed")
def test_s3_jsonlines_pipeline_initialization():
    # Just test that we can initialize the pipeline
    pipeline = S3JsonLinesPipeline(  # type: ignore
        bucket="test-bucket",
        key="data/items.jl",
        region="us-east-1",
    )
    assert pipeline.bucket == "test-bucket"
    assert pipeline.key == "data/items.jl"
    assert pipeline.region == "us-east-1"


@pytest.mark.skipif(not OPENDAL_AVAILABLE, reason="opendal not installed")
async def test_jsonlines_pipeline_appends_with_opendal():
    from silkworm.pipelines import JsonLinesPipeline

    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "items.jl"
        path.write_text('{"existing": true}\n', encoding="utf-8")
        pipeline = JsonLinesPipeline(path, use_opendal=True)
        spider = Spider()

        await pipeline.open(spider)
        await pipeline.process_item({"text": "Hello"}, spider)
        await pipeline.process_item({"text": "World"}, spider)
        # A failed OpenDAL write drops the operator and falls back to a file handle.
        assert pipeline._operator is not None
        assert pipeline._fp is None
        await pipeline.close(spider)

        lines = path.read_text(encoding="utf-8").splitlines()
        assert [json.loads(line) for line in lines] == [
            {"existing": True},
            {"text": "Hello"},
            {"text": "World"},
        ]


# VortexPipeline tests - skip if vortex not installed
try:
    import vortex  # type: ignore[import-not-found]

    from silkworm.pipelines import VortexPipeline

    VORTEX_AVAILABLE = True
except ImportError:
    VORTEX_AVAILABLE = False
    VortexPipeline = None


@pytest.mark.skipif(not VORTEX_AVAILABLE, reason="vortex not installed")
@pytest.mark.skipif(sys.platform == "win32", reason="vortex tests disabled on Windows")
async def test_vortex_pipeline_writes_vortex_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        vortex_path = Path(tmpdir) / "test.vortex"
        pipeline = VortexPipeline(vortex_path)  # type: ignore
        spider = Spider()

        await pipeline.open(spider)
        await pipeline.process_item({"text": "Hello", "author": "John"}, spider)
        await pipeline.process_item({"text": "World", "author": "Jane"}, spider)
        await pipeline.close(spider)

        # Verify Vortex file was created and can be read
        assert vortex_path.exists()
        vortex_file = vortex.file.open(str(vortex_path))
        arrow_reader = vortex_file.to_arrow()
        table = arrow_reader.read_all()

        assert len(table) == 2
        data = table.to_pydict()
        assert data["text"] == ["Hello", "World"]
        assert data["author"] == ["John", "Jane"]


@pytest.mark.skipif(not VORTEX_AVAILABLE, reason="vortex not installed")
@pytest.mark.skipif(sys.platform == "win32", reason="vortex tests disabled on Windows")
async def test_vortex_pipeline_handles_nested_data():
    with tempfile.TemporaryDirectory() as tmpdir:
        vortex_path = Path(tmpdir) / "test.vortex"
        pipeline = VortexPipeline(vortex_path)  # type: ignore
        spider = Spider()

        await pipeline.open(spider)
        await pipeline.process_item(
            {"user": {"name": "Alice", "age": 30}, "tags": ["python", "web"]}, spider
        )
        await pipeline.close(spider)

        # Verify data can be read back
        vortex_file = vortex.file.open(str(vortex_path))
        arrow_reader = vortex_file.to_arrow()
        table = arrow_reader.read_all()

        assert len(table) == 1
        # Vortex/Arrow preserves nested structures


@pytest.mark.skipif(not VORTEX_AVAILABLE, reason="vortex not installed")
@pytest.mark.skipif(sys.platform == "win32", reason="vortex tests disabled on Windows")
async def test_vortex_pipeline_handles_empty():
    with tempfile.TemporaryDirectory() as tmpdir:
        vortex_path = Path(tmpdir) / "test.vortex"
        pipeline = VortexPipeline(vortex_path)  # type: ignore
        spider = Spider()

        await pipeline.open(spider)
        await pipeline.close(spider)

        # File should not be created when no items
        assert not vortex_path.exists()


@pytest.mark.skipif(not VORTEX_AVAILABLE, reason="vortex not installed")
@pytest.mark.skipif(sys.platform == "win32", reason="vortex tests disabled on Windows")
def test_vortex_pipeline_initialization():
    # Just test that we can initialize the pipeline
    pipeline = VortexPipeline("test.vortex")  # type: ignore
    assert pipeline.path == Path("test.vortex")


@pytest.mark.skipif(not VORTEX_AVAILABLE, reason="vortex not installed")
@pytest.mark.skipif(sys.platform == "win32", reason="vortex tests disabled on Windows")
async def test_vortex_pipeline_handles_various_types():
    with tempfile.TemporaryDirectory() as tmpdir:
        vortex_path = Path(tmpdir) / "test.vortex"
        pipeline = VortexPipeline(vortex_path)  # type: ignore
        spider = Spider()

        await pipeline.open(spider)
        await pipeline.process_item(
            {
                "string": "test",
                "integer": 42,
                "float": 3.14,
                "boolean": True,
                "null": None,
                "list": [1, 2, 3],
            },
            spider,
        )
        await pipeline.close(spider)

        # Verify data types are preserved
        vortex_file = vortex.file.open(str(vortex_path))
        arrow_reader = vortex_file.to_arrow()
        table = arrow_reader.read_all()

        assert len(table) == 1
        data = table.to_pydict()
        assert data["string"] == ["test"]
        assert data["integer"] == [42]
        assert abs(data["float"][0] - 3.14) < 0.01
        assert data["boolean"] == [True]
