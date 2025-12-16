# Agent Guide for Silkworm

## Project Overview

**Silkworm** is an async-first web scraping framework built on top of [rnet](https://github.com/0x676e67/rnet) (HTTP client with browser impersonation) and [scraper-rs](https://github.com/RustedBytes/scraper-rs) (fast HTML parsing). It provides a minimal Spider/Request/Response model, middlewares, and pipelines for building web scrapers and crawlers without boilerplate.

### Key Features
- **Async-first architecture** with configurable concurrency and backpressure
- **Browser impersonation** via rnet HTTP client
- **Typed spiders** with callback-based architecture
- **Middleware system** for request/response processing
- **Pipeline system** for data export to various formats and destinations
- **Structured logging** via logly
- **Comprehensive statistics** tracking

### Target Python Versions
- **Python 3.13+** (primary target, specified in `.python-version`)
- **Python 3.14** experimental support (including free-threaded build via `justfile-3.14t`)

## Python 3.13/3.14 Language Features and Best Practices

This project **requires** Python 3.13+ and makes extensive use of modern Python language features. When working on this codebase, you **MUST** use these features appropriately.

### PEP 695: Type Parameter Syntax (Python 3.12+)

**CRITICAL:** This project uses the `type` statement for type aliases. This is the preferred way to define type aliases in Python 3.12+.

#### ✅ Correct Usage (PEP 695)
```python
# From src/silkworm/_types.py
type JSONScalar = str | int | float | bool | None
type JSONValue = JSONScalar | dict[str, JSONValue] | list[JSONValue]
type Headers = dict[str, str]
type MetaData = dict[str, JSONValue]

# From src/silkworm/request.py
type CallbackOutput = (
    Request
    | JSONValue
    | Iterable[Request | JSONValue]
    | AsyncIterable[Request | JSONValue]
    | AsyncIterator[Request | JSONValue]
    | None
)
type CallbackResult = CallbackOutput | Awaitable[CallbackOutput]
type Callback = Callable[["Response"], CallbackResult]
```

#### ❌ Incorrect (Old Style - DO NOT USE)
```python
# Don't use TypeAlias annotation (pre-3.12 style)
from typing import TypeAlias
JSONScalar: TypeAlias = str | int | float | bool | None

# Don't use generic type parameters in classes without PEP 695
from typing import Generic, TypeVar
T = TypeVar('T')
```

**When to use:** Always prefer `type` statements for type aliases in this codebase.

### PEP 636: Structural Pattern Matching (Python 3.10+)

The project uses pattern matching for cleaner conditional logic.

#### ✅ Correct Usage
```python
# From src/silkworm/middlewares.py
match self._strategy:
    case _DelayStrategy.CUSTOM:
        assert self._delay_func is not None
        delay = self._delay_func(request, spider)
    case _DelayStrategy.RANDOM:
        assert self._min_delay is not None and self._max_delay is not None
        delay = random.uniform(self._min_delay, self._max_delay)
    case _DelayStrategy.FIXED:
        assert self._fixed_delay is not None
        delay = self._fixed_delay
    case _:
        assert_never(self._strategy)
```

**When to use:** Use pattern matching for multi-way conditionals based on enums, types, or structured data. Prefer `match` over long `if/elif` chains when checking multiple cases.

### PEP 604: Union Type Operator (Python 3.10+)

**CRITICAL:** Always use the `|` operator for union types instead of `typing.Union`.

#### ✅ Correct Usage
```python
from __future__ import annotations

def process(value: str | int | None) -> Response | Request:
    ...

class Spider:
    def __init__(self, name: str | None = None) -> None:
        ...
```

#### ❌ Incorrect (Old Style - DO NOT USE)
```python
from typing import Union, Optional

def process(value: Union[str, int, None]) -> Union[Response, Request]:
    ...

# Don't use Optional
def __init__(self, name: Optional[str] = None) -> None:
    ...
```

**Important:** Always include `from __future__ import annotations` at the top of every module for forward references and cleaner type hints.

### PEP 585: Type Hinting Generics in Standard Collections (Python 3.9+)

**CRITICAL:** Use built-in collection types directly for type hints, not `typing` module equivalents.

#### ✅ Correct Usage
```python
from collections.abc import AsyncIterator, Iterable, Callable

def process_urls(urls: list[str]) -> dict[str, int]:
    ...

async def generate_items() -> AsyncIterator[dict[str, str]]:
    ...

class Spider:
    start_urls: tuple[str, ...] = ()
```

#### ❌ Incorrect (Old Style - DO NOT USE)
```python
from typing import List, Dict, Tuple

def process_urls(urls: List[str]) -> Dict[str, int]:
    ...
```

**When to use:** Always use `list`, `dict`, `tuple`, `set` directly for type hints. Import abstract types from `collections.abc`, not `typing`.

### PEP 673: Self Type (Python 3.11+)

Use `Self` for methods that return instances of the same class.

#### ✅ Correct Usage
```python
from typing import Self

@dataclass(slots=True)
class Request:
    def replace(self, **kwargs: object) -> Self:
        return replace(self, **kwargs)
```

**When to use:** Use `Self` instead of repeating the class name in return type hints for methods that return the same type.

### PEP 698: Override Decorator (Python 3.12+)

While not currently used in the codebase, consider using `@override` for clarity in subclasses.

```python
from typing import override

class QuotesSpider(Spider):
    @override
    async def parse(self, response: Response) -> CallbackOutput:
        ...
```

### Dataclasses with Slots (Python 3.10+)

**CRITICAL:** Use `slots=True` for better performance and memory efficiency.

#### ✅ Correct Usage
```python
from dataclasses import dataclass, field

@dataclass(slots=True)
class Request:
    url: str
    method: str = "GET"
    headers: Headers = field(default_factory=dict)
    meta: MetaData = field(default_factory=dict)
```

**When to use:** Always use `slots=True` for dataclasses unless you have a specific reason not to (e.g., need to add attributes dynamically).

### Type Narrowing and Type Guards

Use `assert_never` for exhaustive pattern matching checks.

```python
from typing import assert_never

match value:
    case Type1():
        ...
    case Type2():
        ...
    case _:
        assert_never(value)  # Ensures all cases are handled
```

### Protocols (Python 3.8+, but important for this codebase)

Define structural subtyping with Protocols.

```python
from typing import Protocol

class RequestMiddleware(Protocol):
    async def process_request(self, request: Request, spider: Spider) -> Request: ...

class ResponseMiddleware(Protocol):
    async def process_response(
        self,
        response: Response,
        spider: Spider,
    ) -> Response | Request: ...
```

**When to use:** Use Protocols to define interfaces without inheritance, enabling duck typing with type safety.

### Python 3.14 Free-Threaded Build Support

The project has experimental support for Python 3.14's free-threaded build (PEP 703).

```bash
# From justfile-3.14t
PYTHON_GIL=0 uv run python examples/url_titles_spider.py
```

**Note:** When working with free-threaded Python, be aware of:
- Thread safety in shared data structures
- Global Interpreter Lock (GIL) removal implications
- Proper synchronization primitives

## Code Organization and Architecture

### Directory Structure
```
silkworm/
├── src/silkworm/          # Main package
│   ├── __init__.py        # Public API exports
│   ├── _types.py          # Type aliases (using PEP 695)
│   ├── api.py             # Convenience API (fetch_html)
│   ├── engine.py          # Core crawling engine
│   ├── exceptions.py      # Custom exceptions
│   ├── http.py            # HTTP client wrapper
│   ├── logging.py         # Structured logging
│   ├── middlewares.py     # Built-in middlewares
│   ├── pipelines.py       # Built-in item pipelines
│   ├── request.py         # Request dataclass
│   ├── response.py        # Response classes
│   ├── runner.py          # Spider runners (asyncio, uvloop, winloop, trio)
│   └── spiders.py         # Base Spider class
├── examples/              # Example spiders
├── tests/                 # Test suite
├── pyproject.toml         # Project metadata and dependencies
├── justfile               # Development commands (Python 3.13)
├── justfile-3.14t         # Development commands (Python 3.14 free-threaded)
└── uv.lock                # Locked dependencies
```

### Core Components

#### 1. Spider (`spiders.py`)
Base class for all spiders. Subclass and implement `parse()` method.

```python
class Spider:
    name: str = "spider"
    start_urls: tuple[str, ...] = ()
    custom_settings: MetaData = {}
    
    async def parse(self, response: Response) -> CallbackOutput:
        raise NotImplementedError
```

#### 2. Request/Response (`request.py`, `response.py`)
Dataclasses representing HTTP requests and responses.

- `Request`: Immutable request with URL, method, headers, metadata, callback
- `Response`: Base response class
- `HTMLResponse`: Response with HTML parsing capabilities via scraper-rs

#### 3. Middlewares (`middlewares.py`)
Process requests before sending and responses after receiving.

**Request Middlewares:**
- `UserAgentMiddleware`: Rotate/set user agents
- `ProxyMiddleware`: Rotate proxies (round-robin or random)
- `DelayMiddleware`: Add delays (fixed, random, or custom)

**Response Middlewares:**
- `RetryMiddleware`: Retry failed requests with exponential backoff
- `SkipNonHTMLMiddleware`: Skip non-HTML responses

#### 4. Pipelines (`pipelines.py`)
Process and export scraped items.

Built-in pipelines include: JsonLines, SQLite, XML, CSV, MsgPack, Polars, Excel, YAML, Avro, Elasticsearch, MongoDB, MySQL, PostgreSQL, S3, Vortex, Webhook, Google Sheets, Snowflake, FTP, SFTP, Cassandra, CouchDB, DynamoDB, Callback, Taskiq

#### 5. Engine (`engine.py`)
Core async engine managing:
- Concurrent request processing
- Queue management with backpressure
- Middleware and pipeline execution
- Request deduplication
- Statistics tracking

## Development Workflow

### Setup

```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Initialize development environment (Python 3.13)
just init

# Or manually:
uv venv --python python3.13
source .venv/bin/activate  # Windows: .venv\Scripts\activate
uv sync --group dev
```

### Available Commands (via justfile)

```bash
just help          # Show all available commands
just init          # Clean and initialize venv
just fmt           # Format code with ruff
just lint          # Lint code with ruff
just typecheck     # Type check with mypy
just test          # Run tests with pytest
just mypy          # Type check all code (src, tests, examples)
just clean         # Remove all generated files
just build         # Build distribution packages
```

### Code Style and Formatting

**CRITICAL:** Always run formatters and linters before committing.

```bash
# Format code
just fmt

# Check linting
just lint

# Type check
just typecheck

# Run all checks
just fmt && just lint && just typecheck && just test
```

**Ruff Configuration:**
- Follows modern Python best practices
- Automatically formats code
- Checks for common errors and anti-patterns

**Mypy Configuration:**
- Strict type checking enabled
- All code must be properly typed
- Use `TYPE_CHECKING` guard for imports only needed for type hints

### Testing

```bash
# Run all tests
just test

# Run specific test file
uv run --group dev pytest tests/test_specific.py

# Run with verbose output
uv run --group dev pytest -v
```

**Test Framework:** pytest with anyio for async testing

**Important:** Tests use `anyio_mode=auto` to support multiple async backends (asyncio, trio).

## Important Conventions and Best Practices

### 1. Import Organization

Always use this order:
```python
from __future__ import annotations  # ALWAYS FIRST

# Standard library imports
import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING

# Third-party imports
from rnet import Client

# Local imports
from .request import Request
from ._types import MetaData

# TYPE_CHECKING imports
if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from .response import Response
```

### 2. Type Hints

**CRITICAL:** All public APIs MUST have type hints.

```python
# ✅ Good
async def process_request(
    request: Request,
    spider: Spider,
) -> Request | Response:
    ...

# ❌ Bad (no type hints)
async def process_request(request, spider):
    ...
```

### 3. Async/Await

**CRITICAL:** This is an async-first framework. Most public APIs should be async.

```python
# ✅ Good
async def parse(self, response: Response) -> CallbackOutput:
    html = response
    for item in await html.select(".item"):
        yield {"name": item.select_first(".name").text}

# ❌ Bad (blocking operation in async context)
async def parse(self, response: Response) -> CallbackOutput:
    time.sleep(1)  # Don't use blocking sleep in async!
    # Use: await asyncio.sleep(1)
```

### 4. Error Handling

Define custom exceptions in `exceptions.py`:

```python
class SilkwormError(Exception):
    """Base exception for all Silkworm errors."""

class HttpError(SilkwormError):
    """HTTP request failed."""

class SpiderError(SilkwormError):
    """Spider execution error."""
```

### 5. Logging

Use structured logging via logly:

```python
from .logging import get_logger

logger = get_logger(component="MyComponent")
logger.debug("Processing request", url=request.url, method=request.method)
logger.info("Request completed", status_code=response.status_code)
logger.error("Request failed", error=str(e))
```

**Environment Variable:** `SILKWORM_LOG_LEVEL=DEBUG` for verbose output

### 6. Dataclass Best Practices

```python
from dataclasses import dataclass, field

@dataclass(slots=True)  # ALWAYS use slots=True
class MyClass:
    required_field: str
    optional_field: str | None = None
    list_field: list[str] = field(default_factory=list)  # Use factory for mutable defaults
    dict_field: dict[str, int] = field(default_factory=dict)
```

**NEVER** use mutable defaults directly:
```python
# ❌ WRONG - will cause bugs!
@dataclass
class Bad:
    items: list[str] = []  # Shared across all instances!
```

### 7. Protocol Implementation

When implementing protocols, ensure method signatures match exactly:

```python
class MyRequestMiddleware:
    async def process_request(
        self,
        request: Request,
        spider: Spider,
    ) -> Request:  # Return type MUST be Request
        ...
```

### 8. Yielding Items and Requests

Spiders can yield:
- Items (dicts/objects)
- Request objects for follow-up crawling
- Iterables of items/requests
- Async iterables

```python
async def parse(self, response: Response) -> CallbackOutput:
    # Yield single item
    yield {"title": "Example"}
    
    # Yield follow-up request
    yield Request(url="https://example.com/page2", callback=self.parse)
    
    # Yield multiple items
    for item in items:
        yield item
    
    # Or yield iterable
    yield [item1, item2, item3]
```

## Dependencies and Optional Features

### Core Dependencies
- `rnet`: HTTP client with browser impersonation
- `scraper-rust`: Fast HTML parsing
- `logly`: Structured logging
- `rxml`: XML parsing/writing
- `charset-normalizer`: Character encoding detection

### Optional Dependencies (Extras)

Install with `pip install silkworm-rs[extra1,extra2]` or `uv add silkworm-rs --extra extra1 --extra extra2`

Available extras:
- `uvloop`: Fast event loop for Unix-like systems
- `winloop`: Fast event loop for Windows
- `trio`: Trio async backend support
- `msgpack`: MessagePack serialization
- `polars`: Parquet export via Polars
- `excel`: Excel export
- `yaml`: YAML export
- `avro`: Avro format support
- `elasticsearch`: Elasticsearch pipeline
- `mongodb`: MongoDB pipeline
- `mysql`: MySQL pipeline
- `postgresql`: PostgreSQL pipeline
- `s3`: S3 export via OpenDAL
- `vortex`: Vortex columnar format
- `gsheets`: Google Sheets export
- `snowflake`: Snowflake data warehouse
- `ftp`: FTP export
- `sftp`: SFTP export
- `cassandra`: Cassandra database (not available on Windows)
- `couchdb`: CouchDB database
- `dynamodb`: AWS DynamoDB
- `taskiq`: Distributed task queue

## Performance Considerations

### 1. Concurrency Settings
```python
run_spider(
    MySpider,
    concurrency=16,  # Number of concurrent requests
    max_pending_requests=160,  # Queue size (concurrency * 10 default)
    request_timeout=10,  # Per-request timeout
)
```

### 2. Use uvloop for Better Performance
```python
from silkworm import run_spider_uvloop

run_spider_uvloop(MySpider, concurrency=32)  # 2-4x faster
```

### 3. HTML Size Limits
```python
run_spider(
    MySpider,
    html_max_size_bytes=1_000_000,  # Limit HTML parsing size
)
```

### 4. Connection Keep-Alive
```python
run_spider(MySpider, keep_alive=True)  # Reuse connections
```

## Testing Guidelines

### Test Structure
```python
import pytest
from silkworm import Spider, Request, HTMLResponse

class TestMyFeature:
    async def test_something(self):
        # Use async test methods
        result = await async_function()
        assert result == expected
```

### Mocking HTTP Requests
Use test fixtures and mocks for HTTP responses to avoid real network calls.

### Test Coverage
Aim for high test coverage on:
- Core engine logic
- Middleware processing
- Pipeline operations
- Error handling

## Common Pitfalls and How to Avoid Them

### 1. Mutable Default Arguments
❌ **NEVER** do this:
```python
def process(items=[]):  # BUG! Shared across calls
    items.append(1)
```

✅ **DO** this:
```python
def process(items: list[int] | None = None) -> None:
    if items is None:
        items = []
    items.append(1)
```

### 2. Blocking Operations in Async Code
❌ **NEVER** do this:
```python
async def fetch():
    time.sleep(1)  # Blocks the entire event loop!
    requests.get(url)  # Blocks!
```

✅ **DO** this:
```python
async def fetch():
    await asyncio.sleep(1)  # Non-blocking
    # Use async HTTP client (rnet)
```

### 3. Missing `from __future__ import annotations`
❌ **NEVER** forget this:
```python
# Missing import - will cause issues with forward references
class Spider:
    def parse(self) -> Response:  # May fail if Response not defined yet
        ...
```

✅ **DO** this:
```python
from __future__ import annotations  # FIRST LINE!

class Spider:
    def parse(self) -> Response:  # Works with forward references
        ...
```

### 4. Using Old-Style Type Hints
❌ **NEVER** use these:
```python
from typing import List, Dict, Optional, Union

def process(items: Optional[List[str]]) -> Dict[str, Union[int, str]]:
    ...
```

✅ **DO** this:
```python
from __future__ import annotations

def process(items: list[str] | None) -> dict[str, int | str]:
    ...
```

### 5. Request Deduplication
By default, requests with the same URL are deduplicated. To allow duplicates:

```python
yield Request(url=same_url, dont_filter=True)
```

## Version Compatibility Notes

### Python 3.13
- All features fully supported
- Primary development target
- Recommended for production use

### Python 3.14 (Experimental)
- Free-threaded build support (`PYTHON_GIL=0`)
- Use `justfile-3.14t` for development
- May have compatibility issues with some dependencies

### Breaking Changes from Python 3.12
- None expected (3.13+ is the minimum requirement)

## Contributing Guidelines

1. **Fork and clone** the repository
2. **Create a feature branch** from `main`
3. **Make changes** following the code style and conventions above
4. **Run all checks**: `just fmt && just lint && just typecheck && just test`
5. **Commit** with clear, descriptive messages
6. **Open a pull request** with description of changes

### Commit Message Style
```
Add support for custom delay functions in DelayMiddleware

- Implement delay_func parameter
- Update documentation
- Add tests for custom delay strategy
```

## Resources

### Official Documentation
- Python 3.13: https://docs.python.org/3.13/
- Python 3.14: https://docs.python.org/3.14/ (alpha/beta)
- Type Hints: https://peps.python.org/pep-0484/
- PEP 695 (Type Parameters): https://peps.python.org/pep-0695/

### Project Dependencies
- rnet: https://github.com/0x676e67/rnet
- scraper-rs: https://github.com/RustedBytes/scraper-rs
- logly: https://github.com/muhammad-fiaz/logly
- rxml: https://github.com/nephi-dev/rxml

### Development Tools
- uv: https://docs.astral.sh/uv/
- ruff: https://docs.astral.sh/ruff/
- mypy: https://mypy.readthedocs.io/
- pytest: https://docs.pytest.org/

## Summary Checklist for AI Agents

When working on this codebase, ensure you:

- [ ] Use Python 3.13+ syntax exclusively
- [ ] Use `type` statement for type aliases (PEP 695)
- [ ] Use `|` for unions instead of `Union` (PEP 604)
- [ ] Use built-in types (`list`, `dict`) not `typing` equivalents (PEP 585)
- [ ] Use `match`/`case` for multi-way conditionals (PEP 636)
- [ ] Include `from __future__ import annotations` at the top of every module
- [ ] Use `@dataclass(slots=True)` for dataclasses
- [ ] Use `Self` for methods returning same type (PEP 673)
- [ ] Properly type all public APIs
- [ ] Use async/await correctly (no blocking operations)
- [ ] Use structured logging with logly
- [ ] Import from `collections.abc` not `typing` for abstract types
- [ ] Use `field(default_factory=...)` for mutable defaults in dataclasses
- [ ] Run `just fmt && just lint && just typecheck && just test` before committing
- [ ] Follow the import order: `__future__` → stdlib → third-party → local → TYPE_CHECKING

---

**Last Updated:** 2025-12-16  
**Silkworm Version:** 0.2.23  
**Python Requirement:** >=3.13,<3.15
