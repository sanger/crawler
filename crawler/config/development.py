from crawler.config.defaults import *  # noqa: F403,F401

# setting here will overwrite those in 'defaults.py'

# logging config
LOGGING["loggers"]["crawler"]["level"] = "INFO"
LOGGING["loggers"]["crawler"]["handlers"] = ["colored_stream"]
