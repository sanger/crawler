# flake8: noqa
from crawler.config.defaults import *

# setting here will overwrite those in 'defaults.py'

# logging config
LOGGING["loggers"]["crawler"]["level"] = "DEBUG"  # type: ignore
LOGGING["loggers"]["crawler"]["handlers"] = ["colored_stream"]  # type: ignore
