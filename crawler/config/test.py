# flake8: noqa
from crawler.config.defaults import *
from crawler.config.processors import *
from crawler.config.test_centres import *

# settings here overwrite those in 'defaults.py'

###
# general details
###
DIR_DOWNLOADED_DATA = "tests/test_files/good/"

###
# cherrypicker test data options
###
ENABLE_CHERRYPICKER_ENDPOINTS = True

CPTD_FEEDBACK_WAIT_TIME = 2

###
# SFTP details
###
SFTP_UPLOAD = False

###
# MongoDB details
###
MONGO_DB = "crawlerTestDB"
MONGO_URI = f"mongodb://{LOCALHOST}:27017/{MONGO_DB}?replicaSet=heron_rs"

###
# MLWH database details
###
MLWH_DB_DBNAME = "unified_warehouse_test"
MLWH_DB_HOST = LOCALHOST
MLWH_DB_PORT = 3306
MLWH_DB_RO_USER = "root"
MLWH_DB_RO_PASSWORD = ROOT_PASSWORD
MLWH_DB_RW_USER = "root"
MLWH_DB_RW_PASSWORD = ROOT_PASSWORD

WAREHOUSES_RO_CONN_STRING = f"{MLWH_DB_RO_USER}:{MLWH_DB_RO_PASSWORD}@{MLWH_DB_HOST}"
WAREHOUSES_RW_CONN_STRING = f"{MLWH_DB_RW_USER}:{MLWH_DB_RW_PASSWORD}@{MLWH_DB_HOST}"

###
# DART database details
###
DART_DB_DBNAME = "dart_test"
DART_DB_HOST = LOCALHOST
DART_DB_PORT = 1433
DART_DB_RW_USER = "SA"
DART_DB_RW_PASSWORD = "MyS3cr3tPassw0rd"
DART_DB_DRIVER = "{ODBC Driver 17 for SQL Server}"

###
# logging config
###
LOGGING["loggers"]["crawler"]["level"] = "DEBUG"
LOGGING["loggers"]["crawler"]["handlers"] = ["colored_stream_dev"]

###
# MLWH database details
###
ML_WH_DB = "unified_warehouse_test"
MLWH_SAMPLE_TABLE = "sample"
MLWH_STOCK_RESOURCES_TABLE = "stock_resource"
MLWH_STUDY_TABLE = "study"
MLWH_LIGHTHOUSE_SAMPLE_TABLE = "lighthouse_sample"

###
# Event warehouse database details
###
# Only used for setting up test environment
EVENT_WH_SUBJECTS_TABLE = "subjects"
EVENT_WH_ROLES_TABLE = "roles"
EVENT_WH_EVENTS_TABLE = "events"
EVENT_WH_EVENT_TYPES_TABLE = "event_types"
EVENT_WH_SUBJECT_TYPES_TABLE = "subject_types"
EVENT_WH_ROLE_TYPES_TABLE = "role_types"

EVENTS_WH_DB = "event_warehouse_test"

###
# RabbitMQ details
###
RABBITMQ_HOST = ""

###
# APScheduler
###
SCHEDULER_RUN = False
