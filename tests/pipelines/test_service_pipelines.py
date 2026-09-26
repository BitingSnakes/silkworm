from __future__ import annotations

import pytest

from silkworm.pipelines import (
    GOOGLE_SHEETS_AVAILABLE,
    SNOWFLAKE_AVAILABLE,
    GoogleSheetsPipeline,
    SnowflakePipeline,
)
from silkworm.spiders import Spider

# MySQLPipeline tests - skip if aiomysql not installed
try:
    import aiomysql  # type: ignore[import-not-found]  # noqa: F401

    from silkworm.pipelines import MySQLPipeline

    AIOMYSQL_AVAILABLE = True
except ImportError:
    AIOMYSQL_AVAILABLE = False
    MySQLPipeline = None


@pytest.mark.skipif(not AIOMYSQL_AVAILABLE, reason="aiomysql not installed")
def test_mysql_pipeline_initialization():
    # Just test that we can initialize the pipeline
    pipeline = MySQLPipeline(  # type: ignore
        host="localhost",
        port=3306,
        user="root",
        password="password",
        database="test_db",
        table="test_table",
    )
    assert pipeline.host == "localhost"
    assert pipeline.port == 3306
    assert pipeline.user == "root"
    assert pipeline.database == "test_db"
    assert pipeline.table == "test_table"


@pytest.mark.skipif(not AIOMYSQL_AVAILABLE, reason="aiomysql not installed")
def test_mysql_pipeline_invalid_table_name():
    # Test that invalid table names are rejected
    with pytest.raises(ValueError, match="Invalid table name"):
        MySQLPipeline(table="invalid-table-name")  # type: ignore

    with pytest.raises(ValueError, match="Invalid table name"):
        MySQLPipeline(table="123invalid")  # type: ignore

    with pytest.raises(ValueError, match="Invalid table name"):
        MySQLPipeline(table="table; DROP TABLE users;")  # type: ignore


# PostgreSQLPipeline tests - skip if asyncpg not installed
try:
    import asyncpg  # type: ignore[import-not-found]  # noqa: F401

    from silkworm.pipelines import PostgreSQLPipeline

    ASYNCPG_AVAILABLE = True
except ImportError:
    ASYNCPG_AVAILABLE = False
    PostgreSQLPipeline = None


@pytest.mark.skipif(not ASYNCPG_AVAILABLE, reason="asyncpg not installed")
def test_postgresql_pipeline_initialization():
    # Just test that we can initialize the pipeline
    pipeline = PostgreSQLPipeline(  # type: ignore
        host="localhost",
        port=5432,
        user="postgres",
        password="password",
        database="test_db",
        table="test_table",
    )
    assert pipeline.host == "localhost"
    assert pipeline.port == 5432
    assert pipeline.user == "postgres"
    assert pipeline.database == "test_db"
    assert pipeline.table == "test_table"


@pytest.mark.skipif(not ASYNCPG_AVAILABLE, reason="asyncpg not installed")
def test_postgresql_pipeline_invalid_table_name():
    # Test that invalid table names are rejected
    with pytest.raises(ValueError, match="Invalid table name"):
        PostgreSQLPipeline(table="invalid-table-name")  # type: ignore

    with pytest.raises(ValueError, match="Invalid table name"):
        PostgreSQLPipeline(table="123invalid")  # type: ignore

    with pytest.raises(ValueError, match="Invalid table name"):
        PostgreSQLPipeline(table="table; DROP TABLE users;")  # type: ignore


# WebhookPipeline tests
try:
    from silkworm.pipelines import WebhookPipeline

    WEBHOOK_AVAILABLE = True
except ImportError:
    WEBHOOK_AVAILABLE = False
    WebhookPipeline = None


@pytest.mark.skipif(not WEBHOOK_AVAILABLE, reason="wreq not available")
def test_webhook_pipeline_initialization():
    # Test that we can initialize the pipeline
    pipeline = WebhookPipeline(  # type: ignore
        url="https://webhook.site/test",
        method="POST",
        headers={"Authorization": "Bearer token"},
        batch_size=5,
    )
    assert pipeline.url == "https://webhook.site/test"
    assert pipeline.method == "POST"
    assert pipeline.headers == {"Authorization": "Bearer token"}
    assert pipeline.batch_size == 5


@pytest.mark.skipif(not WEBHOOK_AVAILABLE, reason="wreq not available")
async def test_webhook_pipeline_not_opened_raises_error():
    pipeline = WebhookPipeline("https://webhook.site/test")  # type: ignore
    spider = Spider()

    with pytest.raises(RuntimeError, match="WebhookPipeline not opened"):
        await pipeline.process_item({"test": "data"}, spider)


@pytest.mark.skipif(
    not GOOGLE_SHEETS_AVAILABLE, reason="google-api-python-client not installed"
)
def test_google_sheets_pipeline_initialization():
    # Test that we can initialize the pipeline
    pipeline = GoogleSheetsPipeline(
        spreadsheet_id="test_id",
        credentials_file="test_creds.json",
        sheet_name="TestSheet",
        batch_size=50,
    )
    assert pipeline.spreadsheet_id == "test_id"
    assert pipeline.credentials_file == "test_creds.json"
    assert pipeline.sheet_name == "TestSheet"
    assert pipeline.batch_size == 50


@pytest.mark.skipif(
    not GOOGLE_SHEETS_AVAILABLE, reason="google-api-python-client not installed"
)
async def test_google_sheets_pipeline_not_opened_raises_error():
    pipeline = GoogleSheetsPipeline(
        spreadsheet_id="test_id", credentials_file="test_creds.json"
    )
    spider = Spider()

    with pytest.raises(RuntimeError, match="GoogleSheetsPipeline not opened"):
        await pipeline.process_item({"test": "data"}, spider)


@pytest.mark.skipif(
    not SNOWFLAKE_AVAILABLE, reason="snowflake-connector-python not installed"
)
def test_snowflake_pipeline_initialization():
    # Test that we can initialize the pipeline
    pipeline = SnowflakePipeline(
        account="test_account",
        user="test_user",
        password="test_password",
        database="test_db",
        schema="test_schema",
        warehouse="test_warehouse",
        table="test_table",
        role="test_role",
    )
    assert pipeline.account == "test_account"
    assert pipeline.user == "test_user"
    assert pipeline.database == "test_db"
    assert pipeline.schema == "test_schema"
    assert pipeline.warehouse == "test_warehouse"
    assert pipeline.table == "test_table"
    assert pipeline.role == "test_role"


@pytest.mark.skipif(
    not SNOWFLAKE_AVAILABLE, reason="snowflake-connector-python not installed"
)
def test_snowflake_pipeline_invalid_table_name():
    # Test that invalid table names are rejected
    with pytest.raises(ValueError, match="Invalid table name"):
        SnowflakePipeline(
            account="test",
            user="test",
            password="test",
            database="test",
            schema="test",
            warehouse="test",
            table="invalid-table-name",
        )

    with pytest.raises(ValueError, match="Invalid table name"):
        SnowflakePipeline(
            account="test",
            user="test",
            password="test",
            database="test",
            schema="test",
            warehouse="test",
            table="123invalid",
        )


@pytest.mark.skipif(
    not SNOWFLAKE_AVAILABLE, reason="snowflake-connector-python not installed"
)
async def test_snowflake_pipeline_not_opened_raises_error():
    pipeline = SnowflakePipeline(
        account="test_account",
        user="test_user",
        password="test_password",
        database="test_db",
        schema="test_schema",
        warehouse="test_warehouse",
    )
    spider = Spider()

    with pytest.raises(RuntimeError, match="SnowflakePipeline not opened"):
        await pipeline.process_item({"test": "data"}, spider)
