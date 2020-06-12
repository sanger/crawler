from crawler.constants import FIELD_RNA_ID

# general details
DIR_DOWNLOADED_DATA = "data/"

# centre details
CENTRES = [
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": r"^(.*)_([A-Z]\d\d)$",
        "merge_required": True,
        "name": "Alderley",
        "prefix": "ALDP",
        "merge_start_date": "200511",
        "sftp_file_regex": r"^AP_sanger_report_(\d{6}_\d{4})\.csv$",
        "sftp_master_file_regex": r"^AP_sanger_report_(\d{6}_\d{4})_master\.csv$",
        "sftp_root_read": "project-heron_alderly-park",
        "sftp_root_write": "/project-heron_alderly-park/psd-lims",
        "file_names_to_ignore": []
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
        "file_names_to_ignore": ["MK_sanger_report_200610_0001.csv"]
    },
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": r"^(.*)_([A-Z]\d\d)$",
        "merge_required": True,
        "name": "Queen Elizabeth University Hospital",
        "prefix": "QEUH",
        "sftp_file_regex": r"^GLS_sanger_report_(\d{6}_\d{4})\.csv$",
        "sftp_master_file_regex": r"^GLS_sanger_report_(\d{6}_\d{4})_master\.csv$",
        "sftp_root_read": "project-heron_glasgow",
        "sftp_root_write": "/project-heron_glasgow/psd-lims",
        "file_names_to_ignore": []
    },
]

# mongo details
MONGO_DB = "crawlerDevelopmentDB"
MONGO_HOST = "127.0.0.1"
MONGO_PASSWORD = ""
MONGO_PORT = 27017
MONGO_USERNAME = ""

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

# logging config
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "colored": {
            "()": "colorlog.ColoredFormatter",
            "format": "%(asctime)-15s %(name)-16s:%(lineno)-3s %(log_color)s%(levelname)-7s %(message)s",
        },
        "verbose": {
            "format": "%(asctime)-15s %(name)-16s:%(lineno)-3s %(levelname)-7s %(message)s"
        },
    },
    "handlers": {
        "colored_stream": {
            "level": "DEBUG",
            "class": "colorlog.StreamHandler",
            "formatter": "colored",
        },
        "console": {"level": "INFO", "class": "logging.StreamHandler", "formatter": "verbose"},
        "slack": {
            "level": "ERROR",
            "class": "crawler.utils.SlackHandler",
            "formatter": "verbose",
            "token": "",
            "channel_id": "",
        },
    },
    "loggers": {"crawler": {"handlers": ["console", "slack"], "level": "INFO", "propagate": True}},
}
