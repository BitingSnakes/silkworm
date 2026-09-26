# Pipelines

Pipelines process scraped items and write them to files, databases, or external services. They are executed **in order** and each pipeline receives the output of the previous one. See [src/silkworm/pipelines.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/pipelines.py).

## Pipeline Interface
Each pipeline implements three async methods:

```python
class ItemPipeline:
    async def open(self, spider) -> None: ...
    async def close(self, spider) -> None: ...
    async def process_item(self, item, spider): ...
```

The engine calls `open()` once at startup, `process_item()` for every item, and `close()` at shutdown.

## Pipeline Usage

```python
from silkworm.pipelines import JsonLinesPipeline, SQLitePipeline

run_spider(
    MySpider,
    item_pipelines=[
        JsonLinesPipeline("data/items.jl"),
        SQLitePipeline("data/items.db", table="items"),
    ],
)
```

## Streaming vs Buffered Pipelines
- **Streaming (per item)**: `JsonLinesPipeline`, `CSVPipeline`, `XMLPipeline`, `SQLitePipeline`, `WebhookPipeline` (batch_size=1), `ZenohPipeline`.
- **Buffered (write on close)**: `PolarsPipeline`, `ExcelPipeline`, `YAMLPipeline`, `AvroPipeline`, `VortexPipeline`, `S3JsonLinesPipeline`, `FTPPipeline`, `SFTPPipeline`, `RssPipeline`.
- **Batching**: `WebhookPipeline` (batch_size > 1), `GoogleSheetsPipeline`.

> **Note:** Buffered pipelines keep items in memory. Prefer streaming ones for large crawls.

## Optional Dependencies
Pipelines backed by optional extras are always importable; constructing one without its extra installed raises `ImportError` with the install command. Each module also exports an availability flag you can check first:

| Flag | Pipelines |
| --- | --- |
| `FASTAVRO_AVAILABLE` | `AvroPipeline` |
| `CASSANDRA_AVAILABLE` | `CassandraPipeline` |
| `AIOCOUCH_AVAILABLE` | `CouchDBPipeline` |
| `DUCKDB_AVAILABLE` | `DuckDBPipeline` |
| `AIOBOTO3_AVAILABLE` | `DynamoDBPipeline` |
| `ELASTICSEARCH_AVAILABLE` | `ElasticsearchPipeline` |
| `OPENPYXL_AVAILABLE` | `ExcelPipeline` |
| `AIOFTP_AVAILABLE` | `FTPPipeline` |
| `GOOGLE_SHEETS_AVAILABLE` | `GoogleSheetsPipeline` |
| `MOTOR_AVAILABLE` | `MongoDBPipeline` |
| `ORMSGPACK_AVAILABLE` | `MsgPackPipeline` |
| `AIOMYSQL_AVAILABLE` | `MySQLPipeline` |
| `POLARS_AVAILABLE` | `PolarsPipeline` |
| `ASYNCPG_AVAILABLE` | `PostgreSQLPipeline` |
| `OPENDAL_AVAILABLE` | `S3JsonLinesPipeline`, `JsonLinesPipeline(use_opendal=True)` |
| `ASYNCSSH_AVAILABLE` | `SFTPPipeline` |
| `SNOWFLAKE_AVAILABLE` | `SnowflakePipeline` |
| `TASKIQ_AVAILABLE` | `TaskiqPipeline` |
| `VORTEX_AVAILABLE` | `VortexPipeline` |
| `WREQ_AVAILABLE` | `WebhookPipeline` |
| `YAML_AVAILABLE` | `YAMLPipeline` |
| `ZENOH_AVAILABLE` | `ZenohPipeline` |

```python
from silkworm.pipelines import POLARS_AVAILABLE, JsonLinesPipeline, PolarsPipeline

pipeline = PolarsPipeline("data/items.parquet") if POLARS_AVAILABLE else JsonLinesPipeline("data/items.jl")
```

## Per-item Logging
Wrap any pipeline in `LoggedPipeline(pipeline, log_level=...)` to change or silence (`log_level=None`) its per-item log messages. See [Logging and Stats](logging-and-stats.md#pipeline-log-levels).

## Built-in Pipelines

### CallbackPipeline
- **Purpose**: Run a custom callback for each item (sync or async).
- **Behavior**: If the callback returns `None`, the original item passes through unchanged.
- **Options**: `callback` (an `ItemCallback`), `log_level` for per-item log messages.
- **Extras**: none.
- **Code**: [src/silkworm/_pipelines/callback_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/callback_pipeline.py)

```python
from silkworm.pipelines import CallbackPipeline

async def validate_item(item, spider):
    return item

CallbackPipeline(callback=validate_item)
```

### JsonLinesPipeline
- **Purpose**: Write items as JSON Lines to a local file.
- **Options**: `path`, `use_opendal` (async writes with OpenDAL when available), `log_level` for per-item log messages.
- **Extras**: `s3` (OpenDAL).
- **Code**: [src/silkworm/_pipelines/jsonlines_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/jsonlines_pipeline.py)

```python
JsonLinesPipeline("data/items.jl", use_opendal=False)
```

### MsgPackPipeline
- **Purpose**: Binary MessagePack file using `ormsgpack`.
- **Options**: `path`, `mode` (`write` or `append`).
- **Extras**: `msgpack`.
- **Code**: [src/silkworm/_pipelines/msgpack_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/msgpack_pipeline.py)

```python
MsgPackPipeline("data/items.msgpack", mode="append")
```

### SQLitePipeline
- **Purpose**: Store items as JSON text in SQLite.
- **Options**: `path`, `table`.
- **Extras**: none.
- **Code**: [src/silkworm/_pipelines/sqlite_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/sqlite_pipeline.py)

```python
SQLitePipeline("data/items.db", table="quotes")
```

### XMLPipeline
- **Purpose**: Write items as XML with nested data preserved.
- **Options**: `path`, `root_element`, `item_element`.
- **Extras**: none.
- **Code**: [src/silkworm/_pipelines/xml_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/xml_pipeline.py)

```python
XMLPipeline("data/items.xml", root_element="items", item_element="item")
```

### RssPipeline
- **Purpose**: Write items to an RSS 2.0 feed (buffered).
- **Options**: `path`, `channel_title`, `channel_link`, `channel_description`, `max_items` (most recent items kept, default 50; `None` for no limit).
- **Field mappings**: `item_title_field` (default `"title"`), `item_link_field` (`"link"`), `item_description_field` (`"description"`), and optional `item_pub_date_field`, `item_guid_field`, `item_author_field`.
- **Extras**: none.
- **Code**: [src/silkworm/_pipelines/rss_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/rss_pipeline.py)

```python
RssPipeline(
    "data/feed.xml",
    channel_title="My Feed",
    channel_link="https://example.com",
    channel_description="Latest items",
    max_items=50,
)
```

### CSVPipeline
- **Purpose**: CSV export (nested dicts flattened, lists joined by commas).
- **Options**: `path`, `fieldnames` (optional).
- **Extras**: none.
- **Code**: [src/silkworm/_pipelines/csv_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/csv_pipeline.py)

```python
CSVPipeline("data/items.csv", fieldnames=["author", "text", "tags"])
```

### TaskiqPipeline
- **Purpose**: Send items to a Taskiq broker/queue.
- **Options**: `broker`, `task` or `task_name`.
- **Extras**: `taskiq`.
- **Code**: [src/silkworm/_pipelines/taskiq_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/taskiq_pipeline.py)

```python
TaskiqPipeline(broker, task_name=".:process_item")
```

### ZenohPipeline
- **Purpose**: Publish JSON-serialized items to Zenoh immediately.
- **Options**: `key_expr` (static string or a sync/async `ZenohKeyResolver` taking `(item, spider)`), `config` or `session` (mutually exclusive), `encoding` (default `"application/json"`), plus Zenoh publisher QoS options `congestion_control`, `priority`, `express`, `reliability`, and `allowed_destination`.
- **Lifecycle**: Opens and closes its own session by default. An injected `session` remains caller-owned. All publishers created by the pipeline are undeclared on close.
- **Routing**: Dynamic publishers are declared lazily and cached by key expression until close.
- **Extras**: `zenoh`.
- **Code**: [src/silkworm/_pipelines/zenoh_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/zenoh_pipeline.py)

```python
from silkworm.pipelines import ZenohPipeline

ZenohPipeline("scraping/items")
```

### PolarsPipeline
- **Purpose**: Write Parquet via Polars (buffered).
- **Options**: `path`, `mode` (`write` or `append`).
- **Extras**: `polars`.
- **Code**: [src/silkworm/_pipelines/polars_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/polars_pipeline.py)

```python
PolarsPipeline("data/items.parquet", mode="append")
```

### ExcelPipeline
- **Purpose**: Write XLSX via openpyxl (buffered, flattening like CSV).
- **Options**: `path`, `sheet_name`.
- **Extras**: `excel`.
- **Code**: [src/silkworm/_pipelines/excel_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/excel_pipeline.py)

```python
ExcelPipeline("data/items.xlsx", sheet_name="quotes")
```

### YAMLPipeline
- **Purpose**: Write YAML (buffered).
- **Options**: `path`.
- **Extras**: `yaml`.
- **Code**: [src/silkworm/_pipelines/yaml_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/yaml_pipeline.py)

```python
YAMLPipeline("data/items.yaml")
```

### AvroPipeline
- **Purpose**: Write Avro (buffered). Schema can be inferred.
- **Options**: `path`, `schema` (optional).
- **Extras**: `avro`.
- **Code**: [src/silkworm/_pipelines/avro_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/avro_pipeline.py)

```python
AvroPipeline("data/items.avro", schema=my_schema)
```

### ElasticsearchPipeline
- **Purpose**: Index items in Elasticsearch.
- **Options**: `hosts`, `index`, `**es_kwargs`.
- **Extras**: `elasticsearch`.
- **Code**: [src/silkworm/_pipelines/elasticsearch_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/elasticsearch_pipeline.py)

```python
ElasticsearchPipeline(hosts=["http://localhost:9200"], index="quotes")
```

### MongoDBPipeline
- **Purpose**: Insert items into MongoDB.
- **Options**: `connection_string`, `database`, `collection`.
- **Extras**: `mongodb`.
- **Code**: [src/silkworm/_pipelines/mongodb_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/mongodb_pipeline.py)

```python
MongoDBPipeline(database="scraping", collection="items")
```

### S3JsonLinesPipeline
- **Purpose**: Write JSON Lines to S3 via OpenDAL (buffered).
- **Options**: `bucket`, `key`, `region`, optional `endpoint`, `access_key_id`, `secret_access_key`.
- **Extras**: `s3`.
- **Code**: [src/silkworm/_pipelines/s3_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/s3_pipeline.py)

```python
S3JsonLinesPipeline(bucket="my-bucket", key="data/items.jl")
```

### VortexPipeline
- **Purpose**: Write Vortex columnar format (buffered).
- **Options**: `path`.
- **Extras**: `vortex`.
- **Code**: [src/silkworm/_pipelines/vortex_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/vortex_pipeline.py)

```python
VortexPipeline("data/items.vortex")
```

### MySQLPipeline
- **Purpose**: Insert items into MySQL as JSON.
- **Options**: `host`, `port`, `user`, `password`, `database`, `table`.
- **Extras**: `mysql`.
- **Code**: [src/silkworm/_pipelines/mysql_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/mysql_pipeline.py)

```python
MySQLPipeline(database="scraping", table="items")
```

### PostgreSQLPipeline
- **Purpose**: Insert items into PostgreSQL as JSONB.
- **Options**: `host`, `port`, `user`, `password`, `database`, `table`.
- **Extras**: `postgresql`.
- **Code**: [src/silkworm/_pipelines/postgresql_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/postgresql_pipeline.py)

```python
PostgreSQLPipeline(database="scraping", table="items")
```

### WebhookPipeline
- **Purpose**: Send items to a webhook using wreq.
- **Options**: `url`, `method`, `headers`, `timeout`, `batch_size`.
- **Behavior**: If `batch_size` > 1, the payload is a list of items.
- **Extras**: none (wreq is core).
- **Code**: [src/silkworm/_pipelines/webhook_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/webhook_pipeline.py)

```python
WebhookPipeline("https://example.com/webhook", batch_size=10)
```

### GoogleSheetsPipeline
- **Purpose**: Append rows to Google Sheets (batching, flattening like CSV).
- **Options**: `spreadsheet_id`, `credentials_file`, `sheet_name`, `batch_size`.
- **Extras**: `gsheets`.
- **Code**: [src/silkworm/_pipelines/google_sheets_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/google_sheets_pipeline.py)

```python
GoogleSheetsPipeline(
    spreadsheet_id="...",
    credentials_file="creds.json",
    sheet_name="items",
    batch_size=100,
)
```

### SnowflakePipeline
- **Purpose**: Insert items into Snowflake as JSON.
- **Options**: `account`, `user`, `password`, `database`, `schema`, `warehouse`, `table`, `role`.
- **Extras**: `snowflake`.
- **Code**: [src/silkworm/_pipelines/snowflake_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/snowflake_pipeline.py)

```python
SnowflakePipeline(
    account="acct",
    user="user",
    password="pass",
    database="db",
    schema="PUBLIC",
    warehouse="WH",
    table="items",
)
```

### FTPPipeline
- **Purpose**: Upload JSON Lines to FTP (buffered).
- **Options**: `host`, `user`, `password`, `remote_path`, `port`.
- **Extras**: `ftp`.
- **Code**: [src/silkworm/_pipelines/ftp_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/ftp_pipeline.py)

```python
FTPPipeline(host="ftp.example.com", user="user", password="pass")
```

### SFTPPipeline
- **Purpose**: Upload JSON Lines to SFTP (buffered).
- **Options**: `host`, `user`, `password` or `private_key`, `remote_path`, `port`, `known_hosts`, `verify_host_key`.
- **Host key verification**: on by default. The server's host key is checked against `~/.ssh/known_hosts`; pass `known_hosts="path/to/known_hosts"` to use another file. `verify_host_key=False` skips the check. Use it only on trusted networks, since it allows man-in-the-middle attacks. It can't be combined with `known_hosts`.
- **Extras**: `sftp`.
- **Code**: [src/silkworm/_pipelines/sftp_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/sftp_pipeline.py)

```python
SFTPPipeline(host="sftp.example.com", user="user", password="pass")

# Verify against a specific known_hosts file
SFTPPipeline(
    host="sftp.example.com",
    user="user",
    private_key="~/.ssh/id_ed25519",
    known_hosts="deploy/known_hosts",
)
```

> **Behaviour change:** earlier versions never verified the SFTP server's host key. Uploads to a server that isn't in your `known_hosts` file now fail until you add its key (for example with `ssh-keyscan sftp.example.com >> ~/.ssh/known_hosts`), point `known_hosts` at a file that has it, or opt out with `verify_host_key=False`.

### CassandraPipeline
- **Purpose**: Insert items into Cassandra.
- **Options**: `hosts`, `keyspace`, `table`, `username`, `password`, `port`.
- **Extras**: `cassandra` (not available on Windows).
- **Code**: [src/silkworm/_pipelines/cassandra_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/cassandra_pipeline.py)

```python
CassandraPipeline(hosts=["127.0.0.1"], keyspace="scraping", table="items")
```

### CouchDBPipeline
- **Purpose**: Insert items into CouchDB.
- **Options**: `url`, `database`, `username`, `password`.
- **Extras**: `couchdb`.
- **Code**: [src/silkworm/_pipelines/couchdb_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/couchdb_pipeline.py)

```python
CouchDBPipeline(url="http://localhost:5984", database="scraping")
```

### DynamoDBPipeline
- **Purpose**: Insert items into DynamoDB (auto-creates table if missing).
- **Options**: `table_name`, `region_name`, `aws_access_key_id`, `aws_secret_access_key`, `endpoint_url`.
- **Extras**: `dynamodb`.
- **Code**: [src/silkworm/_pipelines/dynamodb_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/dynamodb_pipeline.py)

```python
DynamoDBPipeline(table_name="items", region_name="us-east-1")
```

### DuckDBPipeline
- **Purpose**: Insert items into DuckDB as JSON.
- **Options**: `database`, `table`.
- **Extras**: `duckdb`.
- **Code**: [src/silkworm/_pipelines/duckdb_pipeline.py](https://github.com/BitingSnakes/silkworm/blob/main/src/silkworm/_pipelines/duckdb_pipeline.py)

```python
DuckDBPipeline(database="data/items.db", table="items")
```

## Related Examples
- Callback pipeline: [examples/callback_pipeline_demo.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/callback_pipeline_demo.py)
- Export formats: [examples/export_formats_demo.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/export_formats_demo.py)
- Taskiq pipeline: [examples/taskiq_quotes_spider.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/taskiq_quotes_spider.py)
