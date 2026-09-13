import os
import sys

sys.path.insert(0, os.path.abspath("../.."))

from pyspark_contracts import __version__  # noqa: E402

project = "pyspark-contracts"
copyright = "2026, Vinicius Pereira"
author = "Vinicius Pereira"
release = __version__
version = __version__

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "myst_parser",
]

napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_include_init_with_doc = False

autodoc_member_order = "bysource"
autodoc_typehints = "description"

myst_enable_extensions = ["colon_fence", "deflist"]

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

exclude_patterns = ["_build"]

html_theme = "furo"
