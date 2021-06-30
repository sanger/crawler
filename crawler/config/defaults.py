# flake8: noqa
import os

from crawler.config.centres import *
from crawler.config.logging import *

# setting here will overwrite those in 'centres.py'

# general details

DIR_DOWNLOADED_DATA = "data/sftp_files/"
ADD_LAB_ID = False

# ingest behaviour for scheduled runs
USE_SFTP = True
KEEP_FILES = False
ADD_TO_DART = True

# If we're running in a container, then instead of localhost
# we want host.docker.internal, you can specify this in the
# .env file you use for docker. eg
# LOCALHOST=host.docker.internal
LOCALHOST = os.environ.get("LOCALHOST", "127.0.0.1")
ROOT_PASSWORD = os.environ.get("ROOT_PASSWORD", "")

# mongo details
MONGO_DB = "lighthouseDevelopmentDB"
MONGO_HOST = LOCALHOST
MONGO_PASSWORD = ""
MONGO_PORT = 27017
MONGO_USERNAME = ""

# MLWH database details
MLWH_DB_DBNAME = "unified_warehouse_development"
MLWH_DB_HOST = LOCALHOST
MLWH_DB_PORT = 3306
MLWH_DB_RO_USER = "root"
MLWH_DB_RO_PASSWORD = ROOT_PASSWORD
MLWH_DB_RW_USER = "root"
MLWH_DB_RW_PASSWORD = ROOT_PASSWORD

EVENTS_WH_DB = "event_warehouse_development"

# DART database details
DART_DB_DBNAME = "dart_test"
DART_DB_HOST = os.environ.get("LOCALHOST", "127.0.0.1")
DART_DB_PORT = 1433
DART_DB_RW_USER = "root"
DART_DB_RW_PASSWORD = ""
DART_DB_DRIVER = "{ODBC Driver 17 for SQL Server}"

# SFTP details
SFTP_UPLOAD = False  # upload files to SFTP server
SFTP_HOST = "localhost"
SFTP_PORT = 22
SFTP_READ_PASSWORD = "pass"
SFTP_READ_USERNAME = "foo"
SFTP_WRITE_PASSWORD = "pass"
SFTP_WRITE_USERNAME = "foo"

# slack details
SLACK_API_TOKEN = ""
SLACK_CHANNEL_ID = ""

###
# Eve config
###

# A list of HTTP methods supported at resource endpoints, open to public access even when Authentication and
#   Authorization is enabled.
PUBLIC_METHODS = ["GET"]
PUBLIC_ITEM_METHODS = ["GET"]
DOMAIN: Dict[str, dict] = {"temporary": {}}

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
        "id": "run_crawler",
        "func": "crawler.main:scheduled_run",
        "trigger": "cron",
        "day": "*",
        "hour": "*",
        "minute": "*/15",
    }
]
