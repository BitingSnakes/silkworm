"""Item pipelines.

Implementations live in :mod:`silkworm._pipelines`; this module re-exports
the public API.
"""

from ._pipelines.base import (
    ItemPipeline,
    LoggedPipeline,
)
from ._pipelines.callback_pipeline import CallbackPipeline
from ._pipelines.taskiq_pipeline import (
    TaskiqPipeline,
    TASKIQ_AVAILABLE,
)
from ._pipelines.jsonlines_pipeline import (
    JsonLinesPipeline,
    OPENDAL_AVAILABLE,
)
from ._pipelines.msgpack_pipeline import (
    MsgPackPipeline,
    ORMSGPACK_AVAILABLE,
)
from ._pipelines.sqlite_pipeline import SQLitePipeline
from ._pipelines.xml_pipeline import XMLPipeline
from ._pipelines.rss_pipeline import RssPipeline
from ._pipelines.csv_pipeline import CSVPipeline
from ._pipelines.polars_pipeline import (
    PolarsPipeline,
    POLARS_AVAILABLE,
)
from ._pipelines.excel_pipeline import (
    ExcelPipeline,
    OPENPYXL_AVAILABLE,
)
from ._pipelines.yaml_pipeline import (
    YAMLPipeline,
    YAML_AVAILABLE,
)
from ._pipelines.avro_pipeline import (
    AvroPipeline,
    FASTAVRO_AVAILABLE,
)
from ._pipelines.elasticsearch_pipeline import (
    ElasticsearchPipeline,
    ELASTICSEARCH_AVAILABLE,
)
from ._pipelines.mongodb_pipeline import (
    MongoDBPipeline,
    MOTOR_AVAILABLE,
)
from ._pipelines.s3_pipeline import S3JsonLinesPipeline
from ._pipelines.vortex_pipeline import (
    VortexPipeline,
    VORTEX_AVAILABLE,
)
from ._pipelines.mysql_pipeline import (
    MySQLPipeline,
    AIOMYSQL_AVAILABLE,
)
from ._pipelines.postgresql_pipeline import (
    PostgreSQLPipeline,
    ASYNCPG_AVAILABLE,
)
from ._pipelines.webhook_pipeline import (
    WebhookPipeline,
    WREQ_AVAILABLE,
)
from ._pipelines.google_sheets_pipeline import (
    GoogleSheetsPipeline,
    GOOGLE_SHEETS_AVAILABLE,
)
from ._pipelines.snowflake_pipeline import (
    SnowflakePipeline,
    SNOWFLAKE_AVAILABLE,
)
from ._pipelines.ftp_pipeline import (
    FTPPipeline,
    AIOFTP_AVAILABLE,
)
from ._pipelines.sftp_pipeline import (
    SFTPPipeline,
    ASYNCSSH_AVAILABLE,
)
from ._pipelines.cassandra_pipeline import (
    CassandraPipeline,
    CASSANDRA_AVAILABLE,
)
from ._pipelines.couchdb_pipeline import (
    CouchDBPipeline,
    AIOCOUCH_AVAILABLE,
)
from ._pipelines.dynamodb_pipeline import (
    DynamoDBPipeline,
    AIOBOTO3_AVAILABLE,
)
from ._pipelines.duckdb_pipeline import (
    DuckDBPipeline,
    DUCKDB_AVAILABLE,
)

__all__ = [
    "ItemPipeline",
    "LoggedPipeline",
    "CallbackPipeline",
    "TaskiqPipeline",
    "TASKIQ_AVAILABLE",
    "JsonLinesPipeline",
    "OPENDAL_AVAILABLE",
    "MsgPackPipeline",
    "ORMSGPACK_AVAILABLE",
    "SQLitePipeline",
    "XMLPipeline",
    "RssPipeline",
    "CSVPipeline",
    "PolarsPipeline",
    "POLARS_AVAILABLE",
    "ExcelPipeline",
    "OPENPYXL_AVAILABLE",
    "YAMLPipeline",
    "YAML_AVAILABLE",
    "AvroPipeline",
    "FASTAVRO_AVAILABLE",
    "ElasticsearchPipeline",
    "ELASTICSEARCH_AVAILABLE",
    "MongoDBPipeline",
    "MOTOR_AVAILABLE",
    "S3JsonLinesPipeline",
    "VortexPipeline",
    "VORTEX_AVAILABLE",
    "MySQLPipeline",
    "AIOMYSQL_AVAILABLE",
    "PostgreSQLPipeline",
    "ASYNCPG_AVAILABLE",
    "WebhookPipeline",
    "WREQ_AVAILABLE",
    "GoogleSheetsPipeline",
    "GOOGLE_SHEETS_AVAILABLE",
    "SnowflakePipeline",
    "SNOWFLAKE_AVAILABLE",
    "FTPPipeline",
    "AIOFTP_AVAILABLE",
    "SFTPPipeline",
    "ASYNCSSH_AVAILABLE",
    "CassandraPipeline",
    "CASSANDRA_AVAILABLE",
    "CouchDBPipeline",
    "AIOCOUCH_AVAILABLE",
    "DynamoDBPipeline",
    "AIOBOTO3_AVAILABLE",
    "DuckDBPipeline",
    "DUCKDB_AVAILABLE",
]
