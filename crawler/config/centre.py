# flake8: noqa
from crawler.constants import BIOMEK_LABWARE_CLASS_BIO, BIOMEK_LABWARE_CLASS_KINGFISHER, FIELD_RNA_ID

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
# sftp_file_regex_heron: Regex to identify files to load from the sftp server for project Heron
# sftp_file_regex_eagle: " " for project Eagle
# sftp_master_file_regex: Regexp to identify the master file for incremental updates
# sftp_root_read: directory on sftp from which to load csv files.
# sftp_root_write: directory on sftp in which to upload master files
# file_names_to_ignore: array of files to exclude from processing, such as those
#                       containing invalid headers
CENTRE_REGEX_BARCODE = r"^[\W_]*([\w-]*)_([A-Z]\d{0,1}\d)[\W_]*$"
CENTRE_DIR_BACKUPS = "data/backups"
CENTRE_REGEX_SFTP_FILE_HERON = r"sanger_report_(\d{6}_\d{4}).*\.csv$"
CENTRES = [
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": CENTRE_REGEX_BARCODE,
        "name": "Alderley",
        "prefix": "ALDP",
        "lab_id_default": "AP",
        "backups_folder": f"{CENTRE_DIR_BACKUPS}/ALDP",
        "sftp_file_regex_heron": f"^AP_{CENTRE_REGEX_SFTP_FILE_HERON}",
        "sftp_file_regex_eagle": r"^AP-rna-\d+$",
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
        "sftp_file_regex_heron": f"^MK_{CENTRE_REGEX_SFTP_FILE_HERON}",
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
        "sftp_file_regex_heron": f"^GLS_{CENTRE_REGEX_SFTP_FILE_HERON}",
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
        "sftp_file_regex_heron": f"^CB_{CENTRE_REGEX_SFTP_FILE_HERON}",
        "sftp_root_read": "project-heron_cambridge-az",
        "file_names_to_ignore": ["CB_sanger_report_200714_0001_master.csv"],
        "biomek_labware_class": BIOMEK_LABWARE_CLASS_BIO,
    },
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": CENTRE_REGEX_BARCODE,
        "name": "Randox",
        "prefix": "RAND",
        "lab_id_default": "Randox",
        "backups_folder": f"{CENTRE_DIR_BACKUPS}/RAND",
        "sftp_file_regex_heron": r"^lw-randox-biocentre-box-.*\.csv$",
        "sftp_root_read": "project-heron_randox",
        "file_names_to_ignore": [r"^lw-randox-biocentre-box-((\d)|(1\d)|20)-.*$"],
        "biomek_labware_class": BIOMEK_LABWARE_CLASS_KINGFISHER,
    },
]
