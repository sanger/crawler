from .defaults import *  # noqa: F403,F401

# setting here will overwrite those in 'defaults.py'

# logging config
LOGGING["loggers"]["crawler"]["level"] = "DEBUG"
LOGGING["loggers"]["crawler"]["handlers"] = ["colored_stream"]
