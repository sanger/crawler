# flake8: noqa
from crawler.config.test import *

# setting here will overwrite those in 'test.py

###
# general details
###
DIR_DOWNLOADED_DATA = "tmp/files/"  # Use a different directory so test files don't get deleted for integration tests.

###
# cherrypicker test data options
###
ENABLE_CHERRYPICKER_ENDPOINTS = True
