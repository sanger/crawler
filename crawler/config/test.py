from .defaults import *  # noqa: F403,F401

# settings here overwrite those in defaults.py

# centre details
CENTRES = [
    {
        "name": "Alderley",
        "sftp_root_read": "tests/files/",
        "sftp_file_name": "blah.csv",
        "barcode_field": "RNA ID",
        "barcode_regex": r"^(.*)_([A-Z]\d\d)$",
        "prefix": "ALDP",
    },
    {
        "name": "UK Biocentre",
        "sftp_root_read": "tests/files/",
        "sftp_file_name": "boo.csv",
        "barcode_field": "RNA PLATE ID",
        "barcode_regex": r"",
        "prefix": "MILK",
    },
]

# SFTP details
SFTP_HOST = "127.0.0.1"
SFTP_PASSWORD = "pass"
SFTP_PORT = "22"
SFTP_USER = "foo"

# MongoDB details
MONGO_HOST = "127.0.0.1"
MONGO_DB = "crawlerTestDB"
