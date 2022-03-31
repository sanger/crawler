# flake8: noqa
import os

from crawler.config.centres import CENTRES as centres_config
from crawler.config.logging import *
from crawler.constants import SCHEDULER_JOB_ID_RUN_CRAWLER

# setting here will overwrite those in 'centres.py'

###
# centres config objects
###
CENTRES = centres_config

###
# general details
###
DIR_DOWNLOADED_DATA = "data/sftp_files/"
ADD_LAB_ID = False

###
# cherrypicker test data options
###
ENABLE_CHERRYPICKER_ENDPOINTS = False  # Safeguards it being on by accident in production
MAX_PLATES_PER_TEST_DATA_RUN = 200

###
# ingest behaviour for scheduled runs
###
USE_SFTP = True
KEEP_FILES = False
ADD_TO_DART = True

# If we're running in a container, then instead of localhost
# we want host.docker.internal, you can specify this in the
# .env file you use for docker. eg
# LOCALHOST=host.docker.internal
LOCALHOST = os.environ.get("LOCALHOST", "127.0.0.1")
ROOT_PASSWORD = os.environ.get("ROOT_PASSWORD", "")

###
# Baracoda
###
BARACODA_BASE_URL = "http://uat.baracoda.psd.sanger.ac.uk"

###
# mongo details
###
MONGO_DB = "lighthouseDevelopmentDB"
MONGO_HOST = LOCALHOST
MONGO_PASSWORD = ""
MONGO_PORT = 27017
MONGO_USERNAME = ""

###
# MLWH database details
###
MLWH_DB_DBNAME = "unified_warehouse_development"
MLWH_DB_HOST = LOCALHOST
MLWH_DB_PORT = 3306
MLWH_DB_RO_USER = "root"
MLWH_DB_RO_PASSWORD = ROOT_PASSWORD
MLWH_DB_RW_USER = "root"
MLWH_DB_RW_PASSWORD = ROOT_PASSWORD

EVENTS_WH_DB = "event_warehouse_development"

###
# DART database details
###
DART_DB_DBNAME = "dart_test"
DART_DB_HOST = os.environ.get("LOCALHOST", "127.0.0.1")
DART_DB_PORT = 1433
DART_DB_RW_USER = "sa"
DART_DB_RW_PASSWORD = "MyS3cr3tPassw0rd"
DART_DB_DRIVER = "{ODBC Driver 17 for SQL Server}"

###
# RabbitMQ details
###
RABBITMQ_HOST = os.environ.get("LOCALHOST", "127.0.0.1")
RABBITMQ_USERNAME = ""
RABBITMQ_PASSWORD = ""
RABBITMQ_CRUD_QUEUE = ""

###
# SFTP details
###
SFTP_UPLOAD = False  # upload files to SFTP server
SFTP_HOST = os.environ.get("SFTP_SERVER", "sftp_server")
SFTP_PORT = int(os.environ.get("SFTP_PORT", 22))
SFTP_READ_PASSWORD = "pass"
SFTP_READ_USERNAME = "foo"
SFTP_WRITE_PASSWORD = "pass"
SFTP_WRITE_USERNAME = "foo"

###
# slack details
###
SLACK_API_TOKEN = ""
SLACK_CHANNEL_ID = ""

###
# APScheduler config
###
SCHEDULER_RUN = True
SCHEDULER_TIMEZONE = (
    "Europe/London"  # We need to define timezone because current flask_apscheduler does not load from TZ env
)
SCHEDULER_API_ENABLED = False
JOBS = [
    {
        "id": SCHEDULER_JOB_ID_RUN_CRAWLER,
        "func": "crawler.jobs.apscheduler:scheduled_run",
        "trigger": "cron",
        "day": "*",
        "hour": "*",
        "minute": "10/30",
    }
]
