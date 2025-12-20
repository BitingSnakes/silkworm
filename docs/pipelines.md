# Pipelines

Pipelines process scraped items and write them to files, databases, or external services. They are executed **in order** and each pipeline receives the output of the previous one. See [src/silkworm/pipelines.py](../src/silkworm/pipelines.py).

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
- **Streaming (per item)**: `JsonLinesPipeline`, `CSVPipeline`, `XMLPipeline`, `SQLitePipeline`, `WebhookPipeline` (batch_size=1).
- **Buffered (write on close)**: `PolarsPipeline`, `ExcelPipeline`, `YAMLPipeline`, `AvroPipeline`, `VortexPipeline`, `S3JsonLinesPipeline`, `FTPPipeline`, `SFTPPipeline`.
- **Batching**: `WebhookPipeline` (batch_size > 1), `GoogleSheetsPipeline`.

> **Note:** Buffered pipelines keep items in memory. Prefer streaming ones for large crawls.

## Built-in Pipelines

### CallbackPipeline
- **Purpose**: Run a custom callback for each item (sync or async).
- **Behavior**: If the callback returns `None`, the original item passes through unchanged.
- **Extras**: none.
- **Code**: [src/silkworm/pipelines.py](../src/silkworm/pipelines.py)

```python
from silkworm.pipelines import CallbackPipeline

async def validate_item(item, spider):
    return item

CallbackPipeline(callback=validate_item)
```

### JsonLinesPipeline
- **Purpose**: Write items as JSON Lines to a local file.
- **Options**: `path`, `use_opendal` (async writes with OpenDAL when available).
- **Extras**: `s3` (OpenDAL).
- **Code**: [src/silkworm/pipelines.py](../src/silkworm/pipelines.py)

```python
JsonLinesPipeline("data/items.jl", use_opendal=False)
```

### MsgPackPipeline
- **Purpose**: Binary MessagePack file using `ormsgpack`.
- **Options**: `path`, `mode` (`write` or `append`).
- **Extras**: `msgpack`.
- **Code**: [src/silkworm/pipelines.py](../src/silkworm/pipelines.py)

```python
MsgPackPipeline("data/items.msgpack", mode="append")
```

### SQLitePipeline
- **Purpose**: Store items as JSON text in SQLite.
- **Options**: `path`, `table`.
- **Extras**: none.
- **Code**: [src/silkworm/pipelines.py](../src/silkworm/pipelines.py)

```python
SQLitePipeline("data/items.db", table="quotes")
```

### XMLPipeline
- **Purpose**: Write items as XML with nested data preserved.
- **Options**: `path`, `root_element`, `item_element`.
- **Extras**: none.
- **Code**: [src/silkworm/pipelines.py](../src/silkworm/pipelines.py)

```python
XMLPipeline("data/items.xml", root_element="items", item_element="item")
```

### CSVPipeline
- **Purpose**: CSV export (nested dicts flattened, lists joined by commas).
- **Options**: `path`, `fieldnames` (optional).
- **Extras**: none.
- **Code**: [src/silkworm/pipelines.py](../src/silkworm/pipelines.py)

```python
CSVPipeline("data/items.csv", fieldnames=["author", "text", "tags"])
```

### TaskiqPipeline
- **Purpose**: Send items to a Taskiq broker/queue.
- **Options**: `broker`, `task` or `task_name`.
- **Extras**: `taskiq`.
- **Code**: [src/silkworm/pipelines.py](../src/silkworm/pipelines.py)

```python
TaskiqPipeline(broker, task_name=".:process_item")
```

### PolarsPipeline
- **Purpose**: Write Parquet via Polars (buffered).
- **Options**: `path`, `mode` (`write` or `append`).
- **Extras**: `polars`.
- **Code**: [src/silkworm/pipelines.py](../src/silkworm/pipelines.py)

```python
PolarsPipeline("data/items.parquet", mode="append")
```

### ExcelPipeline
- **Purpose**: Write XLSX via openpyxl (buffered, flattening like CSV).
- **Options**: `path`, `sheet_name`.
- **Extras**: `excel`.
- **Code**: [src/silkworm/pipelines.py](../src/silkworm/pipelines.py)

```python
ExcelPipeline("data/items.xlsx", sheet_name="quotes")
```

### YAMLPipeline
- **Purpose**: Write YAML (buffered).
- **Options**: `path`.
- **Extras**: `yaml`.
- **Code**: [src/silkworm/pipelines.py](../src/silkworm/pipelines.py)

```python
YAMLPipeline("data/items.yaml")
```

### AvroPipeline
- **Purpose**: Write Avro (buffered). Schema can be inferred.
- **Options**: `path`, `schema` (optional).
- **Extras**: `avro`.
- **Code**: [src/silkworm/pipelines.py](../src/silkworm/pipelines.py)

```python
AvroPipeline("data/items.avro", schema=my_schema)
```

### ElasticsearchPipeline
- **Purpose**: Index items in Elasticsearch.
- **Options**: `hosts`, `index`, `**es_kwargs`.
- **Extras**: `elasticsearch`.
- **Code**: [src/silkworm/pipelines.py](../src/silkworm/pipelines.py)

```python
ElasticsearchPipeline(hosts=["http://localhost:9200"], index="quotes")
```

### MongoDBPipeline
- **Purpose**: Insert items into MongoDB.
- **Options**: `connection_string`, `database`, `collection`.
- **Extras**: `mongodb`.
- **Code**: [src/silkworm/pipelines.py](../src/silkworm/pipelines.py)

```python
MongoDBPipeline(database="scraping", collection="items")
```

### S3JsonLinesPipeline
- **Purpose**: Write JSON Lines to S3 via OpenDAL (buffered).
- **Options**: `bucket`, `key`, `region`, optional `endpoint`, `access_key_id`, `secret_access_key`.
- **Extras**: `s3`.
- **Code**: [src/silkworm/pipelines.py](../src/silkworm/pipelines.py)

```python
S3JsonLinesPipeline(bucket="my-bucket", key="data/items.jl")
```

### VortexPipeline
- **Purpose**: Write Vortex columnar format (buffered).
- **Options**: `path`.
- **Extras**: `vortex`.
- **Code**: [src/silkworm/pipelines.py](../src/silkworm/pipelines.py)

```python
VortexPipeline("data/items.vortex")
```

### MySQLPipeline
- **Purpose**: Insert items into MySQL as JSON.
- **Options**: `host`, `port`, `user`, `password`, `database`, `table`.
- **Extras**: `mysql`.
- **Code**: [src/silkworm/pipelines.py](../src/silkworm/pipelines.py)

```python
MySQLPipeline(database="scraping", table="items")
```

### PostgreSQLPipeline
- **Purpose**: Insert items into PostgreSQL as JSONB.
- **Options**: `host`, `port`, `user`, `password`, `database`, `table`.
- **Extras**: `postgresql`.
- **Code**: [src/silkworm/pipelines.py](../src/silkworm/pipelines.py)

```python
PostgreSQLPipeline(database="scraping", table="items")
```

### WebhookPipeline
- **Purpose**: Send items to a webhook using rnet.
- **Options**: `url`, `method`, `headers`, `timeout`, `batch_size`.
- **Behavior**: If `batch_size` > 1, the payload is a list of items.
- **Extras**: none (rnet is core).
- **Code**: [src/silkworm/pipelines.py](../src/silkworm/pipelines.py)

```python
WebhookPipeline("https://example.com/webhook", batch_size=10)
```

### GoogleSheetsPipeline
- **Purpose**: Append rows to Google Sheets (batching, flattening like CSV).
- **Options**: `spreadsheet_id`, `credentials_file`, `sheet_name`, `batch_size`.
- **Extras**: `gsheets`.
- **Code**: [src/silkworm/pipelines.py](../src/silkworm/pipelines.py)

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
- **Code**: [src/silkworm/pipelines.py](../src/silkworm/pipelines.py)

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
- **Code**: [src/silkworm/pipelines.py](../src/silkworm/pipelines.py)

```python
FTPPipeline(host="ftp.example.com", user="user", password="pass")
```

### SFTPPipeline
- **Purpose**: Upload JSON Lines to SFTP (buffered).
- **Options**: `host`, `user`, `password` or `private_key`, `remote_path`, `port`.
- **Extras**: `sftp`.
- **Code**: [src/silkworm/pipelines.py](../src/silkworm/pipelines.py)

```python
SFTPPipeline(host="sftp.example.com", user="user", password="pass")
```

### CassandraPipeline
- **Purpose**: Insert items into Cassandra.
- **Options**: `hosts`, `keyspace`, `table`, `username`, `password`, `port`.
- **Extras**: `cassandra` (not available on Windows).
- **Code**: [src/silkworm/pipelines.py](../src/silkworm/pipelines.py)

```python
CassandraPipeline(hosts=["127.0.0.1"], keyspace="scraping", table="items")
```

### CouchDBPipeline
- **Purpose**: Insert items into CouchDB.
- **Options**: `url`, `database`, `username`, `password`.
- **Extras**: `couchdb`.
- **Code**: [src/silkworm/pipelines.py](../src/silkworm/pipelines.py)

```python
CouchDBPipeline(url="http://localhost:5984", database="scraping")
```

### DynamoDBPipeline
- **Purpose**: Insert items into DynamoDB (auto-creates table if missing).
- **Options**: `table_name`, `region_name`, `aws_access_key_id`, `aws_secret_access_key`, `endpoint_url`.
- **Extras**: `dynamodb`.
- **Code**: [src/silkworm/pipelines.py](../src/silkworm/pipelines.py)

```python
DynamoDBPipeline(table_name="items", region_name="us-east-1")
```

### DuckDBPipeline
- **Purpose**: Insert items into DuckDB as JSON.
- **Options**: `database`, `table`.
- **Extras**: `duckdb`.
- **Code**: [src/silkworm/pipelines.py](../src/silkworm/pipelines.py)

```python
DuckDBPipeline(database="data/items.db", table="items")
```

## Related Examples
- Callback pipeline: [examples/callback_pipeline_demo.py](../examples/callback_pipeline_demo.py)
- Export formats: [examples/export_formats_demo.py](../examples/export_formats_demo.py)
- Taskiq pipeline: [examples/taskiq_quotes_spider.py](../examples/taskiq_quotes_spider.py)
