# Integration Tests

This directory contains integration tests that verify end-to-end functionality using real dependencies (as opposed to the unit tests in the parent directory which use mocked dependencies).

## Test Files

### `test_xpath_integration.py`
Tests XPath functionality using the real `scraper-rs` library to ensure XPath selectors work correctly with actual HTML parsing.

### `test_pipeline_integration.py`
Comprehensive integration tests for all available pipelines. These tests:

- Run a test spider that yields sample data
- Verify each pipeline produces correctly formatted output files
- Validate the content of generated files matches expected data

**Pipelines tested:**

Core pipelines (always available):
- `JsonLinesPipeline` - JSON Lines format
- `CSVPipeline` - CSV format with customizable field names
- `XMLPipeline` - XML format with nested data support
- `SQLitePipeline` - SQLite database storage

Optional pipelines (tested if dependencies installed):
- `MsgPackPipeline` - MessagePack binary format
- `PolarsPipeline` - Parquet format via Polars
- `ExcelPipeline` - Excel XLSX format
- `YAMLPipeline` - YAML format
- `AvroPipeline` - Apache Avro format
- `VortexPipeline` - Vortex columnar format

Database pipelines (tested with Docker containers via testcontainers):
- `MySQLPipeline` - MySQL database storage (requires Docker)
- `PostgreSQLPipeline` - PostgreSQL database storage (requires Docker)
- `MongoDBPipeline` - MongoDB database storage (requires Docker)

Additional test:
- `test_multiple_pipelines_simultaneously()` - Verifies multiple pipelines can run together

## Running Integration Tests

Run all integration tests:
```bash
just test  # or: uv run --group dev pytest -o "anyio_mode=auto"
```

Run only integration tests:
```bash
uv run --group dev pytest tests/integration/ -o "anyio_mode=auto"
```

Run specific integration test:
```bash
uv run --group dev pytest tests/integration/test_pipeline_integration.py -o "anyio_mode=auto"
```

Run with all optional dependencies (to test all pipelines):
```bash
uv sync --group dev --extra msgpack --extra polars --extra excel --extra yaml --extra avro --extra vortex
uv run --group dev pytest tests/integration/test_pipeline_integration.py -o "anyio_mode=auto"
```

Run database integration tests (requires Docker, not available on Windows):
```bash
uv sync --group dev --extra mysql --extra postgresql --extra mongodb
uv run --group dev pytest tests/integration/test_pipeline_integration.py::test_mysql_pipeline_integration -o "anyio_mode=auto"
uv run --group dev pytest tests/integration/test_pipeline_integration.py::test_postgresql_pipeline_integration -o "anyio_mode=auto"
uv run --group dev pytest tests/integration/test_pipeline_integration.py::test_mongodb_pipeline_integration -o "anyio_mode=auto"
```

**Note:** Database integration tests using testcontainers are automatically skipped on Windows platforms as Docker doesn't work well in Windows CI environments.

## Requirements

- Python 3.13-3.14
- Core dependencies: `rnet`, `scraper-rust`, `logly`, `rxml`
- Test dependencies: `pytest`, `anyio`
- Optional dependencies for specific pipeline tests (see `pyproject.toml`)
- Docker (for database integration tests with testcontainers)

## Database Test Containers

The database integration tests use [testcontainers](https://testcontainers-python.readthedocs.io/) to automatically provision and manage Docker containers for MySQL, PostgreSQL, and MongoDB. These tests:

- Automatically start a fresh database container for each test session
- Configure the database pipeline with the container's connection details
- Run the pipeline to insert test data
- Verify the data was correctly stored in the database
- Automatically stop and remove the container after tests complete

The test containers are defined in `conftest.py` as session-scoped fixtures, meaning they are started once per test session and shared across all tests that use them. This improves test performance while maintaining isolation.

**Important:** Testcontainer-based tests are automatically disabled on Windows platforms because Docker doesn't work properly in Windows CI environments. The tests will be skipped with an appropriate message when run on Windows.
