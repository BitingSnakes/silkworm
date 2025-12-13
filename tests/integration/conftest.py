"""
Conftest for integration tests that use real dependencies.

This conftest overrides the parent conftest's dummy modules
to allow integration tests to use the real implementations.
"""

import sys
from pathlib import Path

# Add src to path
ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# Remove the dummy modules installed by parent conftest
# and replace them with real modules (including submodules)
modules_to_reload = ["scraper_rs", "scraper_rs.asyncio", "logly", "rnet", "rxml"]
for module_name in modules_to_reload:
    if module_name in sys.modules:
        del sys.modules[module_name]

# Now import the real modules
try:
    import scraper_rs  # type: ignore[import-untyped]  # noqa: F401
    import logly  # type: ignore[import-untyped]  # noqa: F401
    import rnet  # type: ignore[import-untyped]  # noqa: F401
except ImportError:
    pass  # It's okay if some aren't installed
