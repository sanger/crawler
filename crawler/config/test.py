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
        "sftp_file_name": "blah.csv",
        "sftp_file_regex": r"^AP_sanger_report_(\d{6}_\d{4})\.csv$",
        "sftp_root_read": "tests/files/read",
    },
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": r"^(.*)_([A-Z]\d\d)$",
        "merge_required": True,
        "name": "UK Biocentre",
        "prefix": "MILK",
        "sftp_file_regex": r"^MK_sanger_report_(\d{6}_\d{4})\.csv$",
        "sftp_master_file_regex": r"^MK_sanger_report_(\d{6}_\d{4})_master\.csv$",
        "sftp_root_read": "tests/files/read",
        "sftp_root_write": "tests/files/write",
    },
]

# SFTP details
SFTP_UPLOAD = True
SFTP_HOST = "127.0.0.1"
SFTP_PASSWORD = "pass"
SFTP_PORT = "22"
SFTP_USER = "foo"

# MongoDB details
MONGO_HOST = "127.0.0.1"
MONGO_DB = "crawlerTestDB"
