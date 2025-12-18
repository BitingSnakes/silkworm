"""
Conftest for integration tests that use real dependencies.

This conftest overrides the parent conftest's dummy modules
to allow integration tests to use the real implementations.
"""

import platform
import sys
from pathlib import Path
import pytest

# Add src to path
ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# Remove the dummy modules installed by parent conftest
# and replace them with real modules (including submodules)
modules_to_reload = ["scraper_rs", "scraper_rs.asyncio", "logly", "rnet", "rxml"]
for module_name in modules_to_reload:
    sys.modules.pop(module_name, None)

# Now import the real modules
try:
    import scraper_rs  # type: ignore[import-untyped]  # noqa: F401
    import logly  # type: ignore[import-untyped]  # noqa: F401
    import rnet  # type: ignore[import-untyped]  # noqa: F401
except ImportError:
    pass  # It's okay if some aren't installed


# Test container fixtures for database integration tests
# Testcontainers are disabled on Windows as Docker doesn't work well on Windows CI
IS_WINDOWS = platform.system() == "Windows"

if not IS_WINDOWS:
    try:
        from testcontainers.mysql import MySqlContainer
        from testcontainers.postgres import PostgresContainer
        from testcontainers.mongodb import MongoDbContainer

        TESTCONTAINERS_AVAILABLE = True
    except ImportError:
        TESTCONTAINERS_AVAILABLE = False
else:
    TESTCONTAINERS_AVAILABLE = False


@pytest.fixture(scope="session")
def mysql_container():
    """
    Provide a MySQL test container for integration tests.

    Yields a MySqlContainer instance with connection details.
    The container is automatically started and stopped.
    """
    if not TESTCONTAINERS_AVAILABLE:
        pytest.skip("testcontainers not installed")

    container = MySqlContainer("mysql:8.0")
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def postgres_container():
    """
    Provide a PostgreSQL test container for integration tests.

    Yields a PostgresContainer instance with connection details.
    The container is automatically started and stopped.
    """
    if not TESTCONTAINERS_AVAILABLE:
        pytest.skip("testcontainers not installed")

    container = PostgresContainer("postgres:16")
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def mongodb_container():
    """
    Provide a MongoDB test container for integration tests.

    Yields a MongoDbContainer instance with connection details.
    The container is automatically started and stopped.
    """
    if not TESTCONTAINERS_AVAILABLE:
        pytest.skip("testcontainers not installed")

    container = MongoDbContainer("mongo:7")
    container.start()
    try:
        yield container
    finally:
        container.stop()
