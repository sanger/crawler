# flake8: noqa
import os

from crawler.config.centre import *
from crawler.config.logging import *

# setting here will overwrite those in 'centre.py'

# general details
DIR_DOWNLOADED_DATA = "data/sftp_files/"

ADD_LAB_ID = False

# If we're running in a container, then instead of localhost
# we want host.docker.internal, you can specify this in the
# .env file you use for docker. eg
# LOCALHOST=host.docker.internal
LOCALHOST = os.environ.get("LOCALHOST", "127.0.0.1")
ROOT_PASSWORD = os.environ.get("ROOT_PASSWORD", "")

# mongo details
MONGO_DB = "crawlerDevelopmentDB"
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
