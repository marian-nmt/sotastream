# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------
import sys
from pathlib import Path

DOCS_DIR = Path(__file__).parent.absolute()
PROJECT_DIR = DOCS_DIR.parent
# <root>/docs/conf.py  i.e two levels up ^
SRC_DIR = PROJECT_DIR / 'sotastream'

sys.path.insert(0, str(PROJECT_DIR))
import sotastream  # this import should work

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'Sotastream'
copyright = '2023, Marian NMT'
author = 'Marian NMT'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.mathjax',
    'sphinx.ext.viewcode',
    'sphinx.ext.autodoc',
]

templates_path = ['_templates']
exclude_patterns = []

language = 'en'

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# html_theme = 'alabaster'
html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']

def run_apidoc(_):
    # from sphinx.apidoc import main   # for older Sphinx <= 1.6
    from sphinx.ext.apidoc import main  # for newer
    main(['-e', '-o', str(DOCS_DIR / 'api'), str(SRC_DIR), '--force'])

def setup(app):
    app.connect('builder-inited', run_apidoc)
