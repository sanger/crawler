# flake8: noqa
from crawler.config.defaults import *
from crawler.constants import FIELD_RNA_ID

# settings here overwrite those in 'defaults.py'

# general details
DIR_DOWNLOADED_DATA = "tests/files/"

# change all the backup folder entries for the centres during testing
for centre in CENTRES:
    centre["backups_folder"] = centre["backups_folder"].replace(CENTRE_DIR_BACKUPS, "tmp/backups")

# add a test centre to those defined in defaults.py
CENTRES.append(
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": CENTRE_REGEX_BARCODE,
        "name": "Test Centre",
        "prefix": "TEST",
        "lab_id_default": "TE",
        "backups_folder": "tmp/backups/TEST",
        "sftp_file_regex": f"^TEST_{CENTRE_REGEX_SFTP_FILE}",
        "sftp_root_read": "tests/files",
        "file_names_to_ignore": ["TEST_sanger_report_200518_2205.csv"],
        "biomek_labware_class": BIOMEK_LABWARE_CLASS_KINGFISHER,
    }
)

# SFTP details
SFTP_UPLOAD = False
SFTP_HOST = "127.0.0.1"
SFTP_PASSWORD = "pass"
SFTP_PORT = "22"
SFTP_USER = "foo"

# MongoDB details
MONGO_HOST = "127.0.0.1"
MONGO_DB = "crawlerTestDB"

# MLWH database details
MLWH_DB_DBNAME = "unified_warehouse_test"
MLWH_DB_HOST = "127.0.0.1"
MLWH_DB_PORT = 3306
MLWH_DB_RO_USER = "root"
MLWH_DB_RO_PASSWORD = "root"
MLWH_DB_RW_USER = "root"
MLWH_DB_RW_PASSWORD = "root"

WAREHOUSES_RO_CONN_STRING = f"{MLWH_DB_RO_USER}:{MLWH_DB_RO_PASSWORD}@{LOCALHOST}"
WAREHOUSES_RW_CONN_STRING = f"{MLWH_DB_RW_USER}:{MLWH_DB_RW_PASSWORD}@{LOCALHOST}"

# DART database details
DART_DB_DBNAME = "dart_test"
DART_DB_HOST = "127.0.0.1"
DART_DB_PORT = 1433
DART_DB_RW_USER = "root"
DART_DB_RW_PASSWORD = ""
DART_DB_DRIVER = "{ODBC Driver 17 for SQL Server}"

# logging config
LOGGING["loggers"]["crawler"]["level"] = "DEBUG"  # noqa: F405
LOGGING["loggers"]["crawler"]["handlers"] = ["colored_stream"]  # noqa: F405

# MLWH database details
ML_WH_DB = "unified_warehouse_test"
MLWH_SAMPLE_TABLE = "sample"
MLWH_STOCK_RESOURCES_TABLE = "stock_resource"
MLWH_STUDY_TABLE = "study"

# Event warehouse database details
# Only used for setting up test environment
EVENT_WH_SUBJECTS_TABLE = "subjects"
EVENT_WH_ROLES_TABLE = "roles"
EVENT_WH_EVENTS_TABLE = "events"
EVENT_WH_EVENT_TYPES_TABLE = "event_types"
EVENT_WH_SUBJECT_TYPES_TABLE = "subject_types"
EVENT_WH_ROLE_TYPES_TABLE = "role_types"

EVENTS_WH_DB = "event_warehouse_test"
