# flake8: noqa
from crawler.config.defaults import *
from crawler.constants import FIELD_RNA_ID

# settings here overwrite those in 'defaults.py'

###
# general details
###
DIR_DOWNLOADED_DATA = "tests/test_files/good/"

###
# cherrypicker test data options
###
ENABLE_CHERRYPICKER_ENDPOINTS = True

###
# centres config
###
# change all the backup folder entries for the centres during testing
for centre in CENTRES:
    centre[CENTRE_KEY_BACKUPS_FOLDER] = centre[CENTRE_KEY_BACKUPS_FOLDER].replace(CENTRE_DIR_BACKUPS, "tmp/backups")

# add a test centre to those defined in defaults.py
CENTRES.append(
    {
        CENTRE_KEY_BARCODE_FIELD: FIELD_RNA_ID,
        CENTRE_KEY_BARCODE_REGEX: CENTRE_REGEX_BARCODE,
        CENTRE_KEY_NAME: "Test Centre",
        CENTRE_KEY_PREFIX: "TEST",
        CENTRE_KEY_LAB_ID_DEFAULT: "TE",
        CENTRE_KEY_BACKUPS_FOLDER: "tmp/backups/TEST",
        CENTRE_KEY_FILE_REGEX_UNCONSOLIDATED_SURVEILLANCE: f"^TEST_{CENTRE_REGEX_SFTP_FILE_HERON}",
        "sftp_file_regex_consolidated_surveillance": r"^Test-\d+\.csv$",
        "sftp_file_regex_consolidated_eagle": r"^TE\d+\.csv$",
        "sftp_root_read": "tests/test_files/good",
        "file_names_to_ignore": ["TEST_sanger_report_200518_2205.csv"],
        "biomek_labware_class": BIOMEK_LABWARE_CLASS_KINGFISHER,
        CENTRE_KEY_SKIP_UNCONSOLIDATED_SURVEILLANCE_FILES: False,
        CENTRE_KEY_INCLUDE_IN_SCHEDULED_RUNS: True,
    }
)

###
# SFTP details
###
SFTP_UPLOAD = False

###
# MongoDB details
###
MONGO_HOST = LOCALHOST
MONGO_DB = "crawlerTestDB"

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
# APScheduler
###
SCHEDULER_RUN = False
