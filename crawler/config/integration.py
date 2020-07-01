from .test import *  # noqa: F403,F401

# setting here will overwrite those in 'defaults.py'

# In order to perform integration tests, we want to ensure we don't
# delete our test files, so use a different directory.
DIR_DOWNLOADED_DATA = "tmp/files/"
