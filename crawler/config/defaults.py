from crawler.constants import FIELD_RNA_ID

# centre details
CENTRES = [
    {
        "name": "Alderley",
        "sftp_root_read": "project-heron_alderly-park",
        "sftp_file_regex": r"^AP_sanger_report_(\d{6}_\d{4})\.csv$",
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": r"^(.*)_([A-Z]\d\d)$",
        "prefix": "ALDP",
    },
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": r"^(.*)_([A-Z]\d\d)$",
        "merge_required": True,
        "name": "UK Biocentre",
        "prefix": "MILK",
        "sftp_file_regex": r"^MK_sanger_report_(\d{6}_\d{4})\.csv$",
        "sftp_master_file_regex": r"^MK_sanger_report_(\d{6}_\d{4})_master\.csv$",
        "sftp_root_read": "project-heron/UK-Biocenter/Sanger Reports",
        "sftp_root_write": "/project-heron/psd-lims",
    },
]

# mongo details
MONGO_DB = "crawlerDevelopmentDB"
MONGO_HOST = "127.0.0.1"
MONGO_PASSWORD = ""
MONGO_PORT = 27017
MONGO_USER = ""

# SFTP details
SFTP_HOST = "localhost"
SFTP_PORT = 22
SFTP_READ_PASSWORD = "pass"
SFTP_READ_USER = "foo"
SFTP_WRITE_PASSWORD = "pass"
SFTP_WRITE_USER = "foo"

# slack details
SLACK_API_TOKEN = ""
SLACK_CHANNEL_ID = ""
