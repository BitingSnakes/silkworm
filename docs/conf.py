"""Sphinx configuration for the Silkworm documentation (published to GitHub Pages)."""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version
from pathlib import Path

# Allow building from a checkout without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

project = "Silkworm"
author = "Yehor Smoliakov"
copyright = f"{datetime.now(tz=UTC).year}, {author}"

try:
    release = package_version("silkworm-rs")
except PackageNotFoundError:
    release = "dev"
version = ".".join(release.split(".")[:2])

github_url = "https://github.com/BitingSnakes/silkworm"
github_ref = "main"

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx_copybutton",
]

source_suffix = {".md": "markdown"}
root_doc = "index"
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# -- MyST ---------------------------------------------------------------------

myst_enable_extensions = ["colon_fence", "deflist", "fieldlist"]
myst_heading_anchors = 3

# -- Autodoc ------------------------------------------------------------------

autodoc_default_options = {
    "members": True,
    "member-order": "bysource",
    "show-inheritance": True,
}
autodoc_typehints = "description"
autodoc_typehints_format = "short"
autodoc_class_signature = "separated"
autodoc_preserve_defaults = True
napoleon_google_docstring = True
napoleon_numpy_docstring = False

intersphinx_mapping = {"python": ("https://docs.python.org/3", None)}

# Type hints refer to third-party and private names that have no docs target.
nitpicky = False
suppress_warnings = ["myst.xref_missing"]

# -- HTML ---------------------------------------------------------------------

html_theme = "furo"
html_baseurl = "https://bitingsnakes.github.io/silkworm/"
html_title = f"Silkworm {release}"
html_theme_options = {
    "source_repository": f"{github_url}/",
    "source_branch": github_ref,
    "source_directory": "docs/",
    "footer_icons": [
        {
            "name": "GitHub",
            "url": github_url,
            "html": (
                '<svg stroke="currentColor" fill="currentColor" stroke-width="0" '
                'viewBox="0 0 16 16"><path fill-rule="evenodd" d="M8 0C3.58 0 0 '
                "3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 "
                "0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82"
                "-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 "
                "2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59"
                ".82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 "
                "2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 "
                "2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 "
                "1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 "
                '8c0-4.42-3.58-8-8-8z"></path></svg>'
            ),
            "class": "",
        },
    ],
}
copybutton_prompt_text = r"\$ |>>> "
copybutton_prompt_is_regexp = True
