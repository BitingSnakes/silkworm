"""
Conftest for integration tests that use real dependencies.

This conftest overrides the parent conftest's dummy modules
to allow integration tests to use the real implementations.
"""

import os
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
        import testcontainers  # type: ignore[import-untyped]  # noqa: F401

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

    The MySQL version can be specified using the MYSQL_VERSION environment variable.
    Defaults to "8.0" if not specified.
    """
    if not TESTCONTAINERS_AVAILABLE:
        pytest.skip("testcontainers not installed")

    try:
        from testcontainers.mysql import MySqlContainer  # type: ignore[import-untyped]
    except ImportError:
        pytest.skip("mysql testcontainer dependencies not installed")

    version = os.getenv("MYSQL_VERSION", "8.0")
    container = MySqlContainer(f"mysql:{version}")
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

    The PostgreSQL version can be specified using the POSTGRES_VERSION environment variable.
    Defaults to "16" if not specified.
    """
    if not TESTCONTAINERS_AVAILABLE:
        pytest.skip("testcontainers not installed")

    try:
        from testcontainers.postgres import PostgresContainer  # type: ignore[import-untyped]
    except ImportError:
        pytest.skip("postgres testcontainer dependencies not installed")

    version = os.getenv("POSTGRES_VERSION", "16")
    container = PostgresContainer(f"postgres:{version}")
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

    The MongoDB version can be specified using the MONGODB_VERSION environment variable.
    Defaults to "7" if not specified.
    """
    if not TESTCONTAINERS_AVAILABLE:
        pytest.skip("testcontainers not installed")

    try:
        from testcontainers.mongodb import MongoDbContainer  # type: ignore[import-untyped]
    except ImportError:
        pytest.skip("mongodb testcontainer dependencies not installed")

    version = os.getenv("MONGODB_VERSION", "7")
    container = MongoDbContainer(f"mongo:{version}")
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def elasticsearch_container():
    """
    Provide an Elasticsearch test container for integration tests.

    Yields an ElasticsearchContainer instance with connection details.
    The container is automatically started and stopped.

    The Elasticsearch version can be specified using the ELASTICSEARCH_VERSION environment variable.
    Defaults to "8.11.0" if not specified.
    """
    if not TESTCONTAINERS_AVAILABLE:
        pytest.skip("testcontainers not installed")

    try:
        from testcontainers.elasticsearch import ElasticsearchContainer  # type: ignore[import-untyped]
    except ImportError:
        pytest.skip("elasticsearch testcontainer dependencies not installed")

    version = os.getenv("ELASTICSEARCH_VERSION", "8.11.0")
    container = ElasticsearchContainer(f"elasticsearch:{version}")
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def cassandra_container():
    """
    Provide a Cassandra test container for integration tests.

    Yields a CassandraContainer instance with connection details.
    The container is automatically started and stopped.

    The Cassandra version can be specified using the CASSANDRA_VERSION environment variable.
    Defaults to "4.1" if not specified.
    """
    if not TESTCONTAINERS_AVAILABLE:
        pytest.skip("testcontainers not installed")

    try:
        from testcontainers.cassandra import CassandraContainer  # type: ignore[import-untyped]
    except ImportError:
        pytest.skip("cassandra testcontainer dependencies not installed")

    version = os.getenv("CASSANDRA_VERSION", "4.1")
    container = CassandraContainer(f"cassandra:{version}")
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def couchdb_container():
    """
    Provide a CouchDB test container for integration tests.

    Yields a CouchDbContainer instance with connection details.
    The container is automatically started and stopped.

    The CouchDB version can be specified using the COUCHDB_VERSION environment variable.
    Defaults to "3.3" if not specified.
    """
    if not TESTCONTAINERS_AVAILABLE:
        pytest.skip("testcontainers not installed")

    try:
        from testcontainers.couchdb import CouchDbContainer  # type: ignore[import-untyped]
    except ImportError:
        pytest.skip("couchdb testcontainer dependencies not installed")

    version = os.getenv("COUCHDB_VERSION", "3.3")
    container = CouchDbContainer(f"couchdb:{version}")
    container.start()
    try:
        yield container
    finally:
        container.stop()
