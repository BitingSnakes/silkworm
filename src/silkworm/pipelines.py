"""Item pipelines.

Implementations live in :mod:`silkworm._pipelines`; this module re-exports
the public API.
"""

from ._pipelines.base import (
    ItemPipeline,
    LoggedPipeline,
)
from ._pipelines.callback import CallbackPipeline
from ._pipelines.taskiq import (
    TaskiqPipeline,
    TASKIQ_AVAILABLE,
)
from ._pipelines.jsonlines import (
    JsonLinesPipeline,
    OPENDAL_AVAILABLE,
)
from ._pipelines.msgpack import (
    MsgPackPipeline,
    ORMSGPACK_AVAILABLE,
)
from ._pipelines.sqlite import SQLitePipeline
from ._pipelines.xml import XMLPipeline
from ._pipelines.rss import RssPipeline
from ._pipelines.csv import CSVPipeline
from ._pipelines.polars import (
    PolarsPipeline,
    POLARS_AVAILABLE,
)
from ._pipelines.excel import (
    ExcelPipeline,
    OPENPYXL_AVAILABLE,
)
from ._pipelines.yaml import (
    YAMLPipeline,
    YAML_AVAILABLE,
)
from ._pipelines.avro import (
    AvroPipeline,
    FASTAVRO_AVAILABLE,
)
from ._pipelines.elasticsearch import (
    ElasticsearchPipeline,
    ELASTICSEARCH_AVAILABLE,
)
from ._pipelines.mongodb import (
    MongoDBPipeline,
    MOTOR_AVAILABLE,
)
from ._pipelines.s3 import S3JsonLinesPipeline
from ._pipelines.vortex import (
    VortexPipeline,
    VORTEX_AVAILABLE,
)
from ._pipelines.mysql import (
    MySQLPipeline,
    AIOMYSQL_AVAILABLE,
)
from ._pipelines.postgresql import (
    PostgreSQLPipeline,
    ASYNCPG_AVAILABLE,
)
from ._pipelines.webhook import (
    WebhookPipeline,
    WREQ_AVAILABLE,
)
from ._pipelines.google_sheets import (
    GoogleSheetsPipeline,
    GOOGLE_SHEETS_AVAILABLE,
)
from ._pipelines.snowflake import (
    SnowflakePipeline,
    SNOWFLAKE_AVAILABLE,
)
from ._pipelines.ftp import (
    FTPPipeline,
    AIOFTP_AVAILABLE,
)
from ._pipelines.sftp import (
    SFTPPipeline,
    ASYNCSSH_AVAILABLE,
)
from ._pipelines.cassandra import (
    CassandraPipeline,
    CASSANDRA_AVAILABLE,
)
from ._pipelines.couchdb import (
    CouchDBPipeline,
    AIOCOUCH_AVAILABLE,
)
from ._pipelines.dynamodb import (
    DynamoDBPipeline,
    AIOBOTO3_AVAILABLE,
)
from ._pipelines.duckdb import (
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
