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

## Requirements

- Python 3.13-3.14
- Core dependencies: `rnet`, `scraper-rust`, `logly`, `rxml`
- Test dependencies: `pytest`, `anyio`
- Optional dependencies for specific pipeline tests (see `pyproject.toml`)
