# flake8: noqa
from crawler.config.defaults import *

# setting here will overwrite those in 'defaults.py'

# logging config
LOGGING["loggers"]["crawler"]["level"] = "DEBUG"
LOGGING["loggers"]["crawler"]["handlers"] = ["colored_stream"]

LOGGING["loggers"]["crawler"]["level"] = "DEBUG"  # noqa: F405
LOGGING["loggers"]["crawler"]["handlers"] = ["colored_stream"]  # noqa: F405
