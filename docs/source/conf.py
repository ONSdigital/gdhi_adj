"""Configuration file for Sphinx documentation builder.

For full documentation see:
https://www.sphinx-doc.org/en/master/usage/configuration.html
"""

import os
import sys

# Add the modules to the syspath so that autodoc can detect them.
sys.path.insert(0, os.path.abspath('../..'))

project = 'description_placeholder'
copyright = '2022, Reproducible Data Science & Analysis'
author = 'Reproducible Data Science & Analysis'
release = '1.0.0'

root_doc = 'Main-Modules'

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosectionlabel',
    'sphinx.ext.napoleon',
    'sphinxcontrib.restbuilder'
]
