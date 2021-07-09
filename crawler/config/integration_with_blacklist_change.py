# flake8: noqa
from crawler.config.test import *

# setting here will overwrite those in 'test.py'

# In order to perform integration tests, we want to ensure we don't delete our test files, so use a different directory.
DIR_DOWNLOADED_DATA = "tmp/files/"
ENABLE_CHERRYPICKER_ENDPOINTS = True

# test with no black-listed files
for centre in CENTRES:
    centre["file_names_to_ignore"] = []
