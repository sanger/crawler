from crawler.constants import BIOMEK_LABWARE_CLASS_BIO, BIOMEK_LABWARE_CLASS_KINGFISHER, FIELD_RNA_ID

# general details
DIR_DOWNLOADED_DATA = "data/sftp_files/"

ADD_LAB_ID = False

# centre details
# This information will also be persisted in the mongo database
# Field information:
# barcode_field: The header of the column containing barcode/well information
# barcode_regex: Regular expression for extracting barcodes and well co-ordinates
#                from barcode_field
# merge_required: True for centres delivering incremental updates. Indicates that
#                 the individual csv files need to be merged into a single master
#                 file. False indicates that the latest CSV will contain a full
#                 dump.
# name: The name of the centre
# prefix: The COG-UK prefix. Used for naming the download directory, but also
#         stored in the database for later use by other processes.
#        ie. lighthouse and barcoda
# merge_start_date: Used for centres which switch from full dumps to incremental
#                   updates. Files before this date will be ignored. Please ensure
#                   that at least one complete dump is included in the timeframe.
# sftp_file_regex: Regex to identify files to load from the sftp server
# sftp_master_file_regex: Regexp to identify the master file for incremental updates
# sftp_root_read: directory on sftp from which to load csv files.
# sftp_root_write: directory on sftp in which to upload master files
# file_names_to_ignore: array of files to exclude from processing, such as those
#                       containing invalid headers
CENTRE_REGEX_BARCODE = r"^[\W_]*([\w-]*)_([A-Z]\d{2})[\W_]*$"
CENTRE_DIR_BACKUPS = "data/backups"
CENTRE_REGEX_SFTP_FILE = r"sanger_report_(\d{6}_\d{4}).*\.csv$"
CENTRES = [
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": CENTRE_REGEX_BARCODE,
        "name": "Alderley",
        "prefix": "ALDP",
        "lab_id_default": "AP",
        "backups_folder": f"{CENTRE_DIR_BACKUPS}/ALDP",
        "sftp_file_regex": f"^AP_{CENTRE_REGEX_SFTP_FILE}",
        "sftp_root_read": "project-heron_alderly-park",
        "biomek_labware_class": BIOMEK_LABWARE_CLASS_KINGFISHER,
    },
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": CENTRE_REGEX_BARCODE,
        "name": "UK Biocentre",
        "prefix": "MILK",
        "lab_id_default": "MK",
        "backups_folder": f"{CENTRE_DIR_BACKUPS}/MILK",
        "sftp_file_regex": f"^MK_{CENTRE_REGEX_SFTP_FILE}",
        "sftp_root_read": "project-heron/UK-Biocenter/Sanger Reports",
        "file_names_to_ignore": ["MK_sanger_report_200715_2000_master.csv"],
        "biomek_labware_class": BIOMEK_LABWARE_CLASS_KINGFISHER,
    },
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": CENTRE_REGEX_BARCODE,
        "name": "Queen Elizabeth University Hospital",
        "prefix": "QEUH",
        "lab_id_default": "GLS",
        "backups_folder": f"{CENTRE_DIR_BACKUPS}/QEUH",
        "sftp_file_regex": f"^GLS_{CENTRE_REGEX_SFTP_FILE}",
        "sftp_root_read": "project-heron_glasgow",
        "file_names_to_ignore": ["GLS_sanger_report_200713_0001_master.csv"],
        "biomek_labware_class": BIOMEK_LABWARE_CLASS_KINGFISHER,
    },
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": CENTRE_REGEX_BARCODE,
        "name": "Cambridge-az",
        "prefix": "CAMC",
        "lab_id_default": "CB",
        "backups_folder": f"{CENTRE_DIR_BACKUPS}/CAMC",
        "sftp_file_regex": f"^CB_{CENTRE_REGEX_SFTP_FILE}",
        "sftp_root_read": "project-heron_cambridge-az",
        "file_names_to_ignore": ["CB_sanger_report_200714_0001_master.csv"],
        "biomek_labware_class": BIOMEK_LABWARE_CLASS_BIO,
    },
]

# mongo details
MONGO_DB = "crawlerDevelopmentDB"
MONGO_HOST = "127.0.0.1"
MONGO_PASSWORD = ""
MONGO_PORT = 27017
MONGO_USERNAME = ""

# MLWH database details
MLWH_DB_DBNAME = "unified_warehouse_test"
MLWH_DB_HOST = "127.0.0.1"
MLWH_DB_PORT = 3306
MLWH_DB_RO_USER = "root"
MLWH_DB_RO_PASSWORD = "root"
MLWH_DB_RW_USER = "root"
MLWH_DB_RW_PASSWORD = "root"

# DART database details
DART_DB_DBNAME = "dart_test"
DART_DB_HOST = "127.0.0.1"
DART_DB_PORT = 1433
DART_DB_RW_USER = "root"
DART_DB_RW_PASSWORD = ""
DART_DB_DRIVER = "{ODBC Driver 17 for SQL Server}"

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
            "format": ("%(asctime)-15s %(name)-25s:%(lineno)-3s %(log_color)s%(levelname)-7s %(message)s"),
        },
        "verbose": {"format": "%(asctime)-15s %(name)-25s:%(lineno)-3s %(levelname)-7s %(message)s"},
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
    "loggers": {
        "crawler": {"handlers": ["console", "slack"], "level": "INFO", "propagate": True},
        "migrations": {"handlers": ["colored_stream"], "level": "DEBUG", "propagate": True},
    },
}
