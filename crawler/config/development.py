# flake8: noqa
from crawler.config.defaults import *
from crawler.constants import (RABBITMQ_SUBJECT_CREATE_PLATE,
                               RABBITMQ_SUBJECT_UPDATE_SAMPLE)
from crawler.processing.base_processor import BaseProcessor
from crawler.processing.create_plate_processor import CreatePlateProcessor
from crawler.processing.update_sample_processor import UpdateSampleProcessor

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
LOGGING["loggers"]["apscheduler"]["level"] = "DEBUG"
LOGGING["loggers"]["apscheduler"]["handlers"] = ["colored_stream_dev"]

PROCESSORS: Dict[str, BaseProcessor] = {
    RABBITMQ_SUBJECT_CREATE_PLATE: cast(BaseProcessor, CreatePlateProcessor),
    RABBITMQ_SUBJECT_UPDATE_SAMPLE: cast(BaseProcessor, UpdateSampleProcessor),
}
