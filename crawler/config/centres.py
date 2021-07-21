# flake8: noqa
from typing import Dict, List, Union

from crawler.constants import (
    BIOMEK_LABWARE_CLASS_BIO,
    BIOMEK_LABWARE_CLASS_KINGFISHER,
    FIELD_RNA_ID,
    TEST_DATA_CENTRE_PREFIX,
)

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
# sftp_file_regex_unconsolidated_surveillance: Regex to identify files for unconsolidated plates for project Heron
# sftp_file_regex_consolidated_surveillance: Regex to identify files for consolidated plates for project Heron
# sftp_file_regex_consolidated_eagle: Regex to identify files for consolidated plates for project Eagle
# sftp_master_file_regex: Regexp to identify the master file for incremental updates
# sftp_root_read: directory on sftp from which to load csv files.
# sftp_root_write: directory on sftp in which to upload master files
# file_names_to_ignore: array of files to exclude from processing, such as those
#                       containing invalid headers
# skip_unconsolidated_surveillance_files: Sanger will not be processing unconsolidated files to prevents duplicates

CENTRE_REGEX_BARCODE = r"^[\W_]*([\w-]*)_([A-Z]\d{0,1}\d)[\W_]*$"
CENTRE_DIR_BACKUPS = "data/backups"
CENTRE_REGEX_SFTP_FILE_HERON = r"sanger_report_(\d{6}_\d{4}).*\.csv$"
REGEX_SURVEILLANCE_GLS_1 = r"^GLA\d+[A-Za-z]\.csv$"
REGEX_SURVEILLANCE_GLS_2 = r"^[a-zA-Z]{3}-[a-zA-Z]{2}-\d+\.csv$"

CENTRES_KEY_SKIP_UNCONSOLIDATED_FILES = "skip_unconsolidated_surveillance_files"  # default false
CENTRES_KEY_INCLUDE_IN_BATCH_PROCESS = "include_in_scheduled_runs"  # default true

CENTRES: List[Dict[str, Union[str, bool, List[str]]]] = [
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": CENTRE_REGEX_BARCODE,
        "name": "Alderley",
        "prefix": "ALDP",
        "lab_id_default": "AP",
        "backups_folder": f"{CENTRE_DIR_BACKUPS}/ALDP",
        "sftp_file_regex_unconsolidated_surveillance": f"^AP_{CENTRE_REGEX_SFTP_FILE_HERON}",
        "sftp_file_regex_consolidated_surveillance": r"^[a-zA-Z]{2}-[a-zA-Z]{3}-\d+\.csv$",
        "sftp_file_regex_consolidated_eagle": r"^APE\d+\.csv$",
        "sftp_root_read": "project-heron_alderly-park",
        "biomek_labware_class": BIOMEK_LABWARE_CLASS_KINGFISHER,
        CENTRES_KEY_SKIP_UNCONSOLIDATED_FILES: True,
    },
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": CENTRE_REGEX_BARCODE,
        "name": "UK Biocentre",
        "prefix": "MILK",
        "lab_id_default": "MK",
        "backups_folder": f"{CENTRE_DIR_BACKUPS}/MILK",
        "sftp_file_regex_unconsolidated_surveillance": f"^MK_{CENTRE_REGEX_SFTP_FILE_HERON}",
        "sftp_file_regex_consolidated_surveillance": r"^(cp)?RNA\d+\.csv$",
        "sftp_file_regex_consolidated_eagle": r"^(EGL|EMK)\d+\.csv$",
        "sftp_root_read": "project-heron/UK-Biocenter/Sanger Reports",
        "file_names_to_ignore": ["MK_sanger_report_200715_2000_master.csv"],
        "biomek_labware_class": BIOMEK_LABWARE_CLASS_KINGFISHER,
        CENTRES_KEY_SKIP_UNCONSOLIDATED_FILES: True,
    },
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": CENTRE_REGEX_BARCODE,
        "name": "Queen Elizabeth University Hospital",
        "prefix": "QEUH",
        "lab_id_default": "GLS",
        "backups_folder": f"{CENTRE_DIR_BACKUPS}/QEUH",
        "sftp_file_regex_unconsolidated_surveillance": f"^GLS_{CENTRE_REGEX_SFTP_FILE_HERON}",
        "sftp_file_regex_consolidated_surveillance": f"({REGEX_SURVEILLANCE_GLS_1}|{REGEX_SURVEILLANCE_GLS_2})",
        "sftp_file_regex_consolidated_eagle": r"^(EGG|GLS)\d+\.csv$",
        "sftp_root_read": "project-heron_glasgow",
        "file_names_to_ignore": ["GLS_sanger_report_200713_0001_master.csv"],
        "biomek_labware_class": BIOMEK_LABWARE_CLASS_KINGFISHER,
        CENTRES_KEY_SKIP_UNCONSOLIDATED_FILES: True,
    },
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": CENTRE_REGEX_BARCODE,
        "name": "Cambridge-az",
        "prefix": "CAMC",
        "lab_id_default": "CB",
        "backups_folder": f"{CENTRE_DIR_BACKUPS}/CAMC",
        "sftp_file_regex_unconsolidated_surveillance": f"^CB_{CENTRE_REGEX_SFTP_FILE_HERON}",
        "sftp_file_regex_consolidated_surveillance": r"^\d{9}\.csv$",
        "sftp_file_regex_consolidated_eagle": r"^(EGC|CBE)\d+\.csv$",
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
        "sftp_file_regex_unconsolidated_surveillance": f"^lw-randox-biocentre-box-.*\.csv$",
        "sftp_file_regex_consolidated_surveillance": r"^RDX-[a-zA-Z0-9]{2}-\d+\.csv$",
        "sftp_file_regex_consolidated_eagle": r"^RXE\d+\.csv$",
        "sftp_root_read": "project-heron_randox",
        "file_names_to_ignore": [r"^lw-randox-biocentre-box-((\d)|(1\d)|20)-.*$"],
        "biomek_labware_class": BIOMEK_LABWARE_CLASS_KINGFISHER,
    },
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": CENTRE_REGEX_BARCODE,
        "name": "Health Services Laboratories",
        "prefix": "HSLL",
        "lab_id_default": "HSLL",
        "backups_folder": f"{CENTRE_DIR_BACKUPS}/HSL",
        "sftp_file_regex_unconsolidated_surveillance": r"^$",
        "sftp_file_regex_consolidated_surveillance": r"^HSL\d+\.csv$",
        "sftp_file_regex_consolidated_eagle": r"^$",
        "sftp_root_read": "project-heron_hsl",
        "file_names_to_ignore": [],
        "biomek_labware_class": BIOMEK_LABWARE_CLASS_KINGFISHER,
    },
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": CENTRE_REGEX_BARCODE,
        "name": "Plymouth",
        "prefix": "PLYM",
        "lab_id_default": "PLYM",
        "backups_folder": f"{CENTRE_DIR_BACKUPS}/PLYM",
        "sftp_file_regex_unconsolidated_surveillance": f"^PLYM_{CENTRE_REGEX_SFTP_FILE_HERON}",
        "sftp_file_regex_consolidated_surveillance": r"^PLY-chp-\d+\.csv$",
        "sftp_file_regex_consolidated_eagle": r"^$",
        "sftp_root_read": "project-heron_plym",
        "file_names_to_ignore": [],
        "biomek_labware_class": BIOMEK_LABWARE_CLASS_KINGFISHER,
    },
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": CENTRE_REGEX_BARCODE,
        "name": "Brants Bridge",
        "prefix": "BRBR",
        "lab_id_default": "BRBR",
        "backups_folder": f"{CENTRE_DIR_BACKUPS}/BRBR",
        "sftp_file_regex_unconsolidated_surveillance": r"^$",
        "sftp_file_regex_consolidated_surveillance": r"^BB_\d+\.csv$",
        "sftp_file_regex_consolidated_eagle": r"^$",
        "sftp_root_read": "project-heron_brbr",
        "file_names_to_ignore": [],
        "biomek_labware_class": BIOMEK_LABWARE_CLASS_KINGFISHER,
    },
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": CENTRE_REGEX_BARCODE,
        "name": "Leamington Spa",
        "prefix": "LSPA",
        "lab_id_default": "LML",
        "backups_folder": f"{CENTRE_DIR_BACKUPS}/LSPA",
        "sftp_file_regex_unconsolidated_surveillance": r"^$",
        "sftp_file_regex_consolidated_surveillance": r"^LML_CHERY\d+\.csv$",
        "sftp_file_regex_consolidated_eagle": r"^$",
        "sftp_root_read": "project-heron_lspa",
        "file_names_to_ignore": [],
        "biomek_labware_class": BIOMEK_LABWARE_CLASS_KINGFISHER,
    },
    {
        "barcode_field": FIELD_RNA_ID,
        "barcode_regex": CENTRE_REGEX_BARCODE,
        "name": "Cherrypick Test Data",
        "prefix": TEST_DATA_CENTRE_PREFIX,
        "lab_id_default": "CPTD",
        "backups_folder": f"{CENTRE_DIR_BACKUPS}/CPTD",
        "sftp_file_regex_unconsolidated_surveillance": r"^CPTD_\d{6}_\d{6}_\d{6}\.csv$",
        "sftp_file_regex_consolidated_surveillance": r"^$",
        "sftp_file_regex_consolidated_eagle": r"^$",
        "biomek_labware_class": BIOMEK_LABWARE_CLASS_KINGFISHER,
        CENTRES_KEY_INCLUDE_IN_BATCH_PROCESS: False,
    },
]
