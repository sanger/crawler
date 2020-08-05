from .test import *  # noqa: F403,F401

# setting here will overwrite those in 'defaults.py'

# In order to perform integration tests, we want to ensure we don't
# delete our test files, so use a different directory.
DIR_DOWNLOADED_DATA = "tmp/files/"

# centre details
CENTRES = [
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": r"^(.*)_([A-Z]\d\d)$",
        "name": "Alderley",
        "merge_required": True,
        "prefix": "ALDP",
        "backups_folder": "tmp/backups/ALDP",
        "merge_start_date": "200511",
        "sftp_file_regex": r"^AP_sanger_report_(\d{6}_\d{4})\.csv$",
        "sftp_master_file_regex": r"^AP_sanger_report_(\d{6}_\d{4})_master\.csv$",
        "sftp_root_read": "tests/files",
        "file_names_to_ignore": [],
    },
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": r"^(.*)_([A-Z]\d\d)$",
        "merge_required": True,
        "name": "UK Biocentre",
        "prefix": "MILK",
        "backups_folder": "tmp/backups/MILK",
        "sftp_file_regex": r"^MK_sanger_report_(\d{6}_\d{4})\.csv$",
        "sftp_master_file_regex": r"^MK_sanger_report_(\d{6}_\d{4})_master\.csv$",
        "sftp_root_read": "tests/files",
        "sftp_root_write": "tests/files/write",
        "file_names_to_ignore": [],
    },
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": r"^(.*)_([A-Z]\d\d)$",
        "merge_required": True,
        "name": "Test Centre",
        "prefix": "TEST",
        "backups_folder": "tmp/backups/TEST",
        "sftp_file_regex": r"^TEST_sanger_report_(\d{6}_\d{4})\.csv$",
        "sftp_master_file_regex": r"^TEST_sanger_report_(\d{6}_\d{4})_master\.csv$",
        "sftp_root_read": "tests/files",
        "sftp_root_write": "tests/files/write",
        "file_names_to_ignore": [],
    },
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": r"^(.*)_([A-Z]\d\d)$",
        "merge_required": True,
        "name": "Cambridge-az",
        "prefix": "CAMC",
        "backups_folder": "tmp/backups/CAMC",
        "sftp_file_regex": r"^CB_sanger_report_(\d{6}_\d{4})\.csv$",
        "sftp_master_file_regex": r"^CB_sanger_report_(\d{6}_\d{4})_master\.csv$",
        "sftp_root_read": "project-heron_cambridge-az",
        "sftp_root_write": "/project-heron_cambridge-az/psd-lims",
        "file_names_to_ignore": [],
    },
]
