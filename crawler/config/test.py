from crawler.constants import FIELD_RNA_ID

from .defaults import *  # noqa: F403,F401

# settings here overwrite those in defaults.py

# general details
DIR_DOWNLOADED_DATA = "tests/files/"

# centre details
CENTRES = [
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": r"^(.*)_([A-Z]\d\d)$",
        "name": "Alderley",
        "prefix": "ALDP",
        "lab_id_default": "AP",
        "backups_folder": "tmp/backups/ALDP",
        "sftp_file_regex": r"^AP_sanger_report_(\d{6}_\d{4})\.csv$",
        "sftp_root_read": "tests/files",
        "file_names_to_ignore": [],
    },
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": r"^(.*)_([A-Z]\d\d)$",
        "name": "UK Biocentre",
        "prefix": "MILK",
        "lab_id_default": "MK",
        "backups_folder": "tmp/backups/MILK",
        "sftp_file_regex": r"^MK_sanger_report_(\d{6}_\d{4})\.csv$",
        "sftp_root_read": "tests/files",
        "file_names_to_ignore": [],
    },
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": r"^(.*)_([A-Z]\d\d)$",
        "name": "Test Centre",
        "prefix": "TEST",
        "lab_id_default": "TE",
        "backups_folder": "tmp/backups/TEST",
        "sftp_file_regex": r"^TEST_sanger_report_(\d{6}_\d{4})\.csv$",
        "sftp_root_read": "tests/files",
        "file_names_to_ignore": ["TEST_sanger_report_200518_2205.csv"],
    },
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": r"^(.*)_([A-Z]\d\d)$",
        "name": "Cambridge-az",
        "prefix": "CAMC",
        "lab_id_default": "CB",
        "backups_folder": "tmp/backups/CAMC",
        "sftp_file_regex": r"^CB_sanger_report_(\d{6}_\d{4})\.csv$",
        "sftp_root_read": "project-heron_cambridge-az",
        "file_names_to_ignore": [],
    },
]

# Keep separate from the main tests to avoid continual need
# to update the numbers in 'test_main'.
EXTRA_COLUMN_CENTRE = {
    "barcode_field": FIELD_RNA_ID,
    "barcode_regex": r"^(.*)_([A-Z]\d\d)$",
    "merge_required": True,
    "name": "Test Centre",
    "prefix": "MALF",
    "backups_folder": "tmp/backups/MALF",
    "sftp_file_regex": r"^MALF_sanger_report_(\d{6}_\d{4})\.csv$",
    "sftp_master_file_regex": r"^MALF_sanger_report_(\d{6}_\d{4})_master\.csv$",
    "sftp_root_read": "tests/extra_column_files",
    "sftp_root_write": "tests/extra_column_files/write",
    "file_names_to_ignore": [],
}

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
MLWH_DB_RO_PASSWORD = ""
MLWH_DB_RW_USER = "root"
MLWH_DB_RW_PASSWORD = ""

# logging config
LOGGING["loggers"]["crawler"]["level"] = "DEBUG"
LOGGING["loggers"]["crawler"]["handlers"] = ["colored_stream"]
