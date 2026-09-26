from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

import pytest

from silkworm.spiders import Spider

# CassandraPipeline tests - skip if cassandra-driver not installed or on Windows
if sys.platform == "win32":
    CASSANDRA_AVAILABLE = False
    CassandraPipeline = None  # type: ignore
else:
    try:
        from cassandra.cluster import (  # pyright: ignore[reportMissingImports]
            Cluster,  # noqa: F401
        )

        from silkworm.pipelines import CassandraPipeline

        CASSANDRA_AVAILABLE = True
    except ImportError:
        CASSANDRA_AVAILABLE = False
        CassandraPipeline = None


@pytest.mark.skipif(not CASSANDRA_AVAILABLE, reason="cassandra-driver not installed")
@pytest.mark.skipif(
    sys.platform == "win32", reason="cassandra tests disabled on Windows"
)
def test_cassandra_pipeline_initialization():
    # Test that we can initialize the pipeline
    pipeline = CassandraPipeline(  # type: ignore
        hosts=["127.0.0.1"],
        keyspace="test_keyspace",
        table="test_table",
        username="cassandra",
        password="cassandra",
        port=9042,
    )
    assert pipeline.hosts == ["127.0.0.1"]
    assert pipeline.keyspace == "test_keyspace"
    assert pipeline.table == "test_table"
    assert pipeline.username == "cassandra"
    assert pipeline.password == "cassandra"
    assert pipeline.port == 9042


@pytest.mark.skipif(not CASSANDRA_AVAILABLE, reason="cassandra-driver not installed")
@pytest.mark.skipif(
    sys.platform == "win32", reason="cassandra tests disabled on Windows"
)
def test_cassandra_pipeline_invalid_table_name():
    # Test that invalid table names are rejected
    with pytest.raises(ValueError, match="Invalid table name"):
        CassandraPipeline(table="invalid-table-name")  # type: ignore

    with pytest.raises(ValueError, match="Invalid table name"):
        CassandraPipeline(table="123invalid")  # type: ignore

    with pytest.raises(ValueError, match="Invalid table name"):
        CassandraPipeline(table="table; DROP TABLE users;")  # type: ignore


@pytest.mark.skipif(not CASSANDRA_AVAILABLE, reason="cassandra-driver not installed")
@pytest.mark.skipif(
    sys.platform == "win32", reason="cassandra tests disabled on Windows"
)
async def test_cassandra_pipeline_not_opened_raises_error():
    pipeline = CassandraPipeline(  # type: ignore
        hosts=["127.0.0.1"],
        keyspace="test_keyspace",
        table="test_table",
    )
    spider = Spider()

    with pytest.raises(RuntimeError, match="CassandraPipeline not opened"):
        await pipeline.process_item({"test": "data"}, spider)


# CouchDBPipeline tests - skip if aiocouch not installed
try:
    import aiocouch  # type: ignore[import-not-found]  # noqa: F401

    from silkworm.pipelines import CouchDBPipeline

    AIOCOUCH_AVAILABLE = True
except ImportError:
    AIOCOUCH_AVAILABLE = False
    CouchDBPipeline = None


@pytest.mark.skipif(not AIOCOUCH_AVAILABLE, reason="aiocouch not installed")
def test_couchdb_pipeline_initialization():
    # Test that we can initialize the pipeline
    pipeline = CouchDBPipeline(  # type: ignore
        url="http://localhost:5984",
        database="test_db",
        username="admin",
        password="password",
    )
    assert pipeline.url == "http://localhost:5984"
    assert pipeline.database == "test_db"
    assert pipeline.username == "admin"
    assert pipeline.password == "password"


@pytest.mark.skipif(not AIOCOUCH_AVAILABLE, reason="aiocouch not installed")
async def test_couchdb_pipeline_not_opened_raises_error():
    pipeline = CouchDBPipeline(  # type: ignore
        url="http://localhost:5984",
        database="test_db",
    )
    spider = Spider()

    with pytest.raises(RuntimeError, match="CouchDBPipeline not opened"):
        await pipeline.process_item({"test": "data"}, spider)


# DynamoDBPipeline tests - skip if aioboto3 not installed
try:
    import aioboto3  # type: ignore[import-not-found]  # noqa: F401

    from silkworm.pipelines import DynamoDBPipeline

    AIOBOTO3_AVAILABLE = True
except ImportError:
    AIOBOTO3_AVAILABLE = False
    DynamoDBPipeline = None


@pytest.mark.skipif(not AIOBOTO3_AVAILABLE, reason="aioboto3 not installed")
def test_dynamodb_pipeline_initialization():
    # Test that we can initialize the pipeline
    pipeline = DynamoDBPipeline(  # type: ignore
        table_name="test_table",
        region_name="us-west-2",
        aws_access_key_id="test_key",
        aws_secret_access_key="test_secret",
        endpoint_url="http://localhost:8000",
    )
    assert pipeline.table_name == "test_table"
    assert pipeline.region_name == "us-west-2"
    assert pipeline.aws_access_key_id == "test_key"
    assert pipeline.aws_secret_access_key == "test_secret"
    assert pipeline.endpoint_url == "http://localhost:8000"


@pytest.mark.skipif(not AIOBOTO3_AVAILABLE, reason="aioboto3 not installed")
async def test_dynamodb_pipeline_not_opened_raises_error():
    pipeline = DynamoDBPipeline(  # type: ignore
        table_name="test_table",
        region_name="us-east-1",
    )
    spider = Spider()

    with pytest.raises(RuntimeError, match="DynamoDBPipeline not opened"):
        await pipeline.process_item({"test": "data"}, spider)


# DuckDBPipeline tests - skip if duckdb not installed
try:
    import duckdb  # type: ignore[import-not-found]

    from silkworm.pipelines import DuckDBPipeline

    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False
    DuckDBPipeline = None


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="duckdb not installed")
async def test_duckdb_pipeline_writes_items():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        pipeline = DuckDBPipeline(db_path, table="items")  # type: ignore
        spider = Spider()

        await pipeline.open(spider)
        await pipeline.process_item({"text": "Hello", "author": "John"}, spider)
        await pipeline.process_item({"text": "World", "author": "Jane"}, spider)
        await pipeline.close(spider)

        # Read and verify data
        conn = duckdb.connect(str(db_path))
        result = conn.execute("SELECT spider, data FROM items ORDER BY id").fetchall()
        conn.close()

        assert len(result) == 2
        assert result[0][0] == "spider"  # Default spider name
        assert json.loads(result[0][1]) == {"text": "Hello", "author": "John"}
        assert result[1][0] == "spider"
        assert json.loads(result[1][1]) == {"text": "World", "author": "Jane"}


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="duckdb not installed")
async def test_duckdb_pipeline_handles_nested_data():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        pipeline = DuckDBPipeline(db_path)  # type: ignore
        spider = Spider()

        await pipeline.open(spider)
        await pipeline.process_item(
            {"user": {"name": "Alice", "age": 30}, "tags": ["python", "web"]}, spider
        )
        await pipeline.close(spider)

        # Read and verify
        conn = duckdb.connect(str(db_path))
        result = conn.execute("SELECT data FROM items").fetchone()
        conn.close()

        data = json.loads(result[0])
        assert data == {"user": {"name": "Alice", "age": 30}, "tags": ["python", "web"]}


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="duckdb not installed")
async def test_duckdb_pipeline_not_opened_raises_error():
    pipeline = DuckDBPipeline("test.db")  # type: ignore
    spider = Spider()

    with pytest.raises(RuntimeError, match="DuckDBPipeline not opened"):
        await pipeline.process_item({"test": "data"}, spider)


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="duckdb not installed")
def test_duckdb_pipeline_invalid_table_name():
    # Test that invalid table names are rejected
    with pytest.raises(ValueError, match="Invalid table name"):
        DuckDBPipeline(table="invalid-table-name")  # type: ignore

    with pytest.raises(ValueError, match="Invalid table name"):
        DuckDBPipeline(table="123invalid")  # type: ignore

    with pytest.raises(ValueError, match="Invalid table name"):
        DuckDBPipeline(table="table; DROP TABLE users;")  # type: ignore


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="duckdb not installed")
def test_duckdb_pipeline_initialization():
    # Test that we can initialize the pipeline
    pipeline = DuckDBPipeline(  # type: ignore
        database="data/test.db",
        table="test_table",
    )
    assert pipeline.database == Path("data/test.db")
    assert pipeline.table == "test_table"


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="duckdb not installed")
async def test_duckdb_pipeline_creates_parent_directories():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "subdir" / "test.db"
        pipeline = DuckDBPipeline(db_path)  # type: ignore
        spider = Spider()

        await pipeline.open(spider)
        await pipeline.process_item({"text": "Test"}, spider)
        await pipeline.close(spider)

        # Verify database file was created
        assert db_path.exists()


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="duckdb not installed")
async def test_duckdb_pipeline_persistent_across_sessions():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        spider = Spider()

        # First session: write items
        pipeline1 = DuckDBPipeline(db_path)  # type: ignore
        await pipeline1.open(spider)
        await pipeline1.process_item({"text": "First"}, spider)
        await pipeline1.close(spider)

        # Second session: write more items
        pipeline2 = DuckDBPipeline(db_path)  # type: ignore
        await pipeline2.open(spider)
        await pipeline2.process_item({"text": "Second"}, spider)
        await pipeline2.close(spider)

        # Verify both items are present
        conn = duckdb.connect(str(db_path))
        result = conn.execute("SELECT COUNT(*) FROM items").fetchone()
        conn.close()

        assert result[0] == 2
