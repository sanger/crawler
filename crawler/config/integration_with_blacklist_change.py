from crawler.config.test import *  # noqa: F403,F401

# setting here will overwrite those in 'defaults.py'

# In order to perform integration tests, we want to ensure we don't
# delete our test files, so use a different directory.
DIR_DOWNLOADED_DATA = "tmp/files/"

# centre details
CENTRES = [
    {
        "barcode_field": FIELD_RNA_ID,  # noqa: F405
        "barcode_regex": r"^(.*)_([A-Z]\d\d)$",
        "name": "Alderley",
        "prefix": "ALDP",
        "lab_id_default": "AP",
        "backups_folder": "tmp/backups/ALDP",
        "sftp_file_regex": r"^AP_sanger_report_(\d{6}_\d{4})\.csv$",
        "sftp_root_read": "tests/files",
        "file_names_to_ignore": [],
        "biomek_labware_class": "KingFisher_96_2ml",
    },
    {
        "barcode_field": FIELD_RNA_ID,  # noqa: F405
        "barcode_regex": r"^(.*)_([A-Z]\d\d)$",
        "name": "UK Biocentre",
        "prefix": "MILK",
        "lab_id_default": "MK",
        "backups_folder": "tmp/backups/MILK",
        "sftp_file_regex": r"^MK_sanger_report_(\d{6}_\d{4})\.csv$",
        "sftp_root_read": "tests/files",
        "file_names_to_ignore": [],
        "biomek_labware_class": "KingFisher_96_2ml",
    },
    {
        "barcode_field": FIELD_RNA_ID,  # noqa: F405
        "barcode_regex": r"^(.*)_([A-Z]\d\d)$",
        "name": "Test Centre",
        "prefix": "TEST",
        "lab_id_default": "TS",
        "backups_folder": "tmp/backups/TEST",
        "sftp_file_regex": r"^TEST_sanger_report_(\d{6}_\d{4})\.csv$",
        "sftp_root_read": "tests/files",
        "file_names_to_ignore": [],
        "biomek_labware_class": "KingFisher_96_2ml",
    },
    {
        "barcode_field": FIELD_RNA_ID,  # noqa: F405
        "barcode_regex": r"^(.*)_([A-Z]\d\d)$",
        "name": "Cambridge-az",
        "prefix": "CAMC",
        "lab_id_default": "CB",
        "backups_folder": "tmp/backups/CAMC",
        "sftp_file_regex": r"^CB_sanger_report_(\d{6}_\d{4})\.csv$",
        "sftp_root_read": "project-heron_cambridge-az",
        "file_names_to_ignore": [],
        "biomek_labware_class": "Bio-Rad_96PCR",
    },
]
