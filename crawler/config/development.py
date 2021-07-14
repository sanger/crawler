# flake8: noqa
from crawler.config.defaults import *

# setting here will overwrite those in 'defaults.py'

###
# cherrypicker test data options
###
ENABLE_CHERRYPICKER_ENDPOINTS = True

###
# ingest behaviour for scheduled runs
###
USE_SFTP = False
KEEP_FILES = True
ADD_TO_DART = False

###
# logging config
###
LOGGING["loggers"]["crawler"]["level"] = "DEBUG"
LOGGING["loggers"]["crawler"]["handlers"] = ["colored_stream_dev"]
