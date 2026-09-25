"""Item pipelines.

Implementations live in :mod:`silkworm._pipelines`; this module re-exports
the public API.
"""

from ._pipelines.avro_pipeline import (
    FASTAVRO_AVAILABLE,
    AvroPipeline,
)
from ._pipelines.base import (
    ItemPipeline,
    LoggedPipeline,
)
from ._pipelines.callback_pipeline import CallbackPipeline
from ._pipelines.cassandra_pipeline import (
    CASSANDRA_AVAILABLE,
    CassandraPipeline,
)
from ._pipelines.couchdb_pipeline import (
    AIOCOUCH_AVAILABLE,
    CouchDBPipeline,
)
from ._pipelines.csv_pipeline import CSVPipeline
from ._pipelines.duckdb_pipeline import (
    DUCKDB_AVAILABLE,
    DuckDBPipeline,
)
from ._pipelines.dynamodb_pipeline import (
    AIOBOTO3_AVAILABLE,
    DynamoDBPipeline,
)
from ._pipelines.elasticsearch_pipeline import (
    ELASTICSEARCH_AVAILABLE,
    ElasticsearchPipeline,
)
from ._pipelines.excel_pipeline import (
    OPENPYXL_AVAILABLE,
    ExcelPipeline,
)
from ._pipelines.ftp_pipeline import (
    AIOFTP_AVAILABLE,
    FTPPipeline,
)
from ._pipelines.google_sheets_pipeline import (
    GOOGLE_SHEETS_AVAILABLE,
    GoogleSheetsPipeline,
)
from ._pipelines.jsonlines_pipeline import (
    OPENDAL_AVAILABLE,
    JsonLinesPipeline,
)
from ._pipelines.mongodb_pipeline import (
    MOTOR_AVAILABLE,
    MongoDBPipeline,
)
from ._pipelines.msgpack_pipeline import (
    ORMSGPACK_AVAILABLE,
    MsgPackPipeline,
)
from ._pipelines.mysql_pipeline import (
    AIOMYSQL_AVAILABLE,
    MySQLPipeline,
)
from ._pipelines.polars_pipeline import (
    POLARS_AVAILABLE,
    PolarsPipeline,
)
from ._pipelines.postgresql_pipeline import (
    ASYNCPG_AVAILABLE,
    PostgreSQLPipeline,
)
from ._pipelines.rss_pipeline import RssPipeline
from ._pipelines.s3_pipeline import S3JsonLinesPipeline
from ._pipelines.sftp_pipeline import (
    ASYNCSSH_AVAILABLE,
    SFTPPipeline,
)
from ._pipelines.snowflake_pipeline import (
    SNOWFLAKE_AVAILABLE,
    SnowflakePipeline,
)
from ._pipelines.sqlite_pipeline import SQLitePipeline
from ._pipelines.taskiq_pipeline import (
    TASKIQ_AVAILABLE,
    TaskiqPipeline,
)
from ._pipelines.vortex_pipeline import (
    VORTEX_AVAILABLE,
    VortexPipeline,
)
from ._pipelines.webhook_pipeline import (
    WREQ_AVAILABLE,
    WebhookPipeline,
)
from ._pipelines.xml_pipeline import XMLPipeline
from ._pipelines.yaml_pipeline import (
    YAML_AVAILABLE,
    YAMLPipeline,
)

__all__ = [
    "AIOBOTO3_AVAILABLE",
    "AIOCOUCH_AVAILABLE",
    "AIOFTP_AVAILABLE",
    "AIOMYSQL_AVAILABLE",
    "ASYNCPG_AVAILABLE",
    "ASYNCSSH_AVAILABLE",
    "CASSANDRA_AVAILABLE",
    "DUCKDB_AVAILABLE",
    "ELASTICSEARCH_AVAILABLE",
    "FASTAVRO_AVAILABLE",
    "GOOGLE_SHEETS_AVAILABLE",
    "MOTOR_AVAILABLE",
    "OPENDAL_AVAILABLE",
    "OPENPYXL_AVAILABLE",
    "ORMSGPACK_AVAILABLE",
    "POLARS_AVAILABLE",
    "SNOWFLAKE_AVAILABLE",
    "TASKIQ_AVAILABLE",
    "VORTEX_AVAILABLE",
    "WREQ_AVAILABLE",
    "YAML_AVAILABLE",
    "AvroPipeline",
    "CSVPipeline",
    "CallbackPipeline",
    "CassandraPipeline",
    "CouchDBPipeline",
    "DuckDBPipeline",
    "DynamoDBPipeline",
    "ElasticsearchPipeline",
    "ExcelPipeline",
    "FTPPipeline",
    "GoogleSheetsPipeline",
    "ItemPipeline",
    "JsonLinesPipeline",
    "LoggedPipeline",
    "MongoDBPipeline",
    "MsgPackPipeline",
    "MySQLPipeline",
    "PolarsPipeline",
    "PostgreSQLPipeline",
    "RssPipeline",
    "S3JsonLinesPipeline",
    "SFTPPipeline",
    "SQLitePipeline",
    "SnowflakePipeline",
    "TaskiqPipeline",
    "VortexPipeline",
    "WebhookPipeline",
    "XMLPipeline",
    "YAMLPipeline",
]
