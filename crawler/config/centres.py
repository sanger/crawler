from typing import Final, List

from crawler.constants import (
    BIOMEK_LABWARE_CLASS_BIO,
    BIOMEK_LABWARE_CLASS_KINGFISHER,
    FIELD_RNA_ID,
    TEST_DATA_CENTRE_PREFIX,
)
from crawler.types import CentreConf

# Centre Details
#
# This information is persisted in the MongoDB as the single source of truth, but where the Mongo collection is missing,
# it will be recreated from the values listed here.
#
# Field information:
#   barcode_field:                               The header of the column containing barcode/well information.
#   barcode_regex:                               Regular expression for extracting barcodes and well co-ordinates from
#                                                barcode_field.
#   merge_required:                              True for centres delivering incremental updates. Indicates that the
#                                                individual csv files need to be merged into a single master file. False
#                                                indicates that the latest CSV will contain a full dump.
#   name:                                        The name of the centre.
#   prefix:                                      The COG-UK prefix. Used for naming the download directory, but also
#                                                stored in the database for later use by
#                                                other processes. ie. lighthouse and barcoda.
#   merge_start_date:                            Used for centres which switch from full dumps to incremental updates.
#                                                Files before this date will be ignored. Please ensure that at least one
#                                                complete dump is included in the timeframe.
#   sftp_file_regex_unconsolidated_surveillance: Regex to identify files for unconsolidated plates for project Heron.
#   sftp_file_regex_consolidated_surveillance:   Regex to identify files for consolidated plates for project Heron.
#   sftp_file_regex_consolidated_eagle:          Regex to identify files for consolidated plates for project Eagle.
#   sftp_master_file_regex:                      Regexp to identify the master file for incremental updates.
#   sftp_root_read:                              Directory on sftp from which to load csv files.
#   sftp_root_write:                             Directory on sftp in which to upload master files.
#   file_names_to_ignore:                        Array of files to exclude from processing, such as those containing
#                                                invalid headers.
#   skip_unconsolidated_surveillance_files:      Sanger will not be processing unconsolidated files to prevents
#                                                duplicates.
#   include_in_scheduled_runs:                   True when a centre should be processed as part of a batch run of all
#                                                centres.

CENTRE_REGEX_BARCODE = r"^[\W_]*([\w-]*)_([A-Z]\d{0,1}\d)[\W_]*$"
CENTRE_DIR_BACKUPS = "data/backups"
CENTRE_REGEX_SFTP_FILE_HERON = r"sanger_report_(\d{6}_\d{4}).*\.csv$"
REGEX_SURVEILLANCE_GLS_1 = r"^GLA\d+[A-Za-z]\.csv$"
REGEX_SURVEILLANCE_GLS_2 = r"^[a-zA-Z]{3}-[a-zA-Z]{2}-\d+\.csv$"

CENTRE_KEY_BACKUPS_FOLDER: Final = "backups_folder"
CENTRE_KEY_BARCODE_FIELD: Final = "barcode_field"
CENTRE_KEY_BARCODE_REGEX: Final = "barcode_regex"
CENTRE_KEY_BIOMEK_LABWARE_CLASS: Final = "biomek_labware_class"
CENTRE_KEY_FILE_NAMES_TO_IGNORE: Final = "file_names_to_ignore"
CENTRE_KEY_FILE_REGEX_CONSOLIDATED_EAGLE: Final = "sftp_file_regex_consolidated_eagle"
CENTRE_KEY_FILE_REGEX_CONSOLIDATED_SURVEILLANCE: Final = "sftp_file_regex_consolidated_surveillance"
CENTRE_KEY_FILE_REGEX_UNCONSOLIDATED_SURVEILLANCE: Final = "sftp_file_regex_unconsolidated_surveillance"
CENTRE_KEY_INCLUDE_IN_SCHEDULED_RUNS: Final = "include_in_scheduled_runs"
CENTRE_KEY_LAB_ID_DEFAULT: Final = "lab_id_default"
CENTRE_KEY_NAME: Final = "name"
CENTRE_KEY_PREFIX: Final = "prefix"
CENTRE_KEY_SFTP_ROOT_READ: Final = "sftp_root_read"
CENTRE_KEY_SKIP_UNCONSOLIDATED_SURVEILLANCE_FILES: Final = "skip_unconsolidated_surveillance_files"

CENTRES: List[CentreConf] = [
    {
        CENTRE_KEY_BACKUPS_FOLDER: f"{CENTRE_DIR_BACKUPS}/ALDP",
        CENTRE_KEY_BARCODE_FIELD: FIELD_RNA_ID,
        CENTRE_KEY_BARCODE_REGEX: CENTRE_REGEX_BARCODE,
        CENTRE_KEY_BIOMEK_LABWARE_CLASS: BIOMEK_LABWARE_CLASS_KINGFISHER,
        CENTRE_KEY_FILE_NAMES_TO_IGNORE: [],
        CENTRE_KEY_FILE_REGEX_CONSOLIDATED_EAGLE: r"^APE\d+\.csv$",
        CENTRE_KEY_FILE_REGEX_CONSOLIDATED_SURVEILLANCE: r"^[a-zA-Z]{2}-[a-zA-Z]{3}-\d+\.csv$",
        CENTRE_KEY_FILE_REGEX_UNCONSOLIDATED_SURVEILLANCE: f"^AP_{CENTRE_REGEX_SFTP_FILE_HERON}",
        CENTRE_KEY_INCLUDE_IN_SCHEDULED_RUNS: True,
        CENTRE_KEY_LAB_ID_DEFAULT: "AP",
        CENTRE_KEY_NAME: "Alderley",
        CENTRE_KEY_PREFIX: "ALDP",
        CENTRE_KEY_SFTP_ROOT_READ: "project-heron_alderly-park",
        CENTRE_KEY_SKIP_UNCONSOLIDATED_SURVEILLANCE_FILES: True,
    },
    {
        CENTRE_KEY_BACKUPS_FOLDER: f"{CENTRE_DIR_BACKUPS}/MILK",
        CENTRE_KEY_BARCODE_FIELD: FIELD_RNA_ID,
        CENTRE_KEY_BARCODE_REGEX: CENTRE_REGEX_BARCODE,
        CENTRE_KEY_BIOMEK_LABWARE_CLASS: BIOMEK_LABWARE_CLASS_KINGFISHER,
        CENTRE_KEY_FILE_NAMES_TO_IGNORE: ["MK_sanger_report_200715_2000_master.csv"],
        CENTRE_KEY_FILE_REGEX_CONSOLIDATED_EAGLE: r"^(EGL|EMK)\d+\.csv$",
        CENTRE_KEY_FILE_REGEX_CONSOLIDATED_SURVEILLANCE: r"^(cp)?RNA\d+\.csv$",
        CENTRE_KEY_FILE_REGEX_UNCONSOLIDATED_SURVEILLANCE: f"^MK_{CENTRE_REGEX_SFTP_FILE_HERON}",
        CENTRE_KEY_INCLUDE_IN_SCHEDULED_RUNS: True,
        CENTRE_KEY_LAB_ID_DEFAULT: "MK",
        CENTRE_KEY_NAME: "UK Biocentre",
        CENTRE_KEY_PREFIX: "MILK",
        CENTRE_KEY_SFTP_ROOT_READ: "project-heron/UK-Biocenter/Sanger Reports",
        CENTRE_KEY_SKIP_UNCONSOLIDATED_SURVEILLANCE_FILES: True,
    },
    {
        CENTRE_KEY_BACKUPS_FOLDER: f"{CENTRE_DIR_BACKUPS}/QEUH",
        CENTRE_KEY_BARCODE_FIELD: FIELD_RNA_ID,
        CENTRE_KEY_BARCODE_REGEX: CENTRE_REGEX_BARCODE,
        CENTRE_KEY_BIOMEK_LABWARE_CLASS: BIOMEK_LABWARE_CLASS_KINGFISHER,
        CENTRE_KEY_FILE_NAMES_TO_IGNORE: ["GLS_sanger_report_200713_0001_master.csv"],
        CENTRE_KEY_FILE_REGEX_CONSOLIDATED_EAGLE: r"^(EGG|GLS)\d+\.csv$",
        CENTRE_KEY_FILE_REGEX_CONSOLIDATED_SURVEILLANCE: f"({REGEX_SURVEILLANCE_GLS_1}|{REGEX_SURVEILLANCE_GLS_2})",
        CENTRE_KEY_FILE_REGEX_UNCONSOLIDATED_SURVEILLANCE: f"^GLS_{CENTRE_REGEX_SFTP_FILE_HERON}",
        CENTRE_KEY_INCLUDE_IN_SCHEDULED_RUNS: True,
        CENTRE_KEY_LAB_ID_DEFAULT: "GLS",
        CENTRE_KEY_NAME: "Queen Elizabeth University Hospital",
        CENTRE_KEY_PREFIX: "QEUH",
        CENTRE_KEY_SFTP_ROOT_READ: "project-heron_glasgow",
        CENTRE_KEY_SKIP_UNCONSOLIDATED_SURVEILLANCE_FILES: True,
    },
    {
        CENTRE_KEY_BACKUPS_FOLDER: f"{CENTRE_DIR_BACKUPS}/CAMC",
        CENTRE_KEY_BARCODE_FIELD: FIELD_RNA_ID,
        CENTRE_KEY_BARCODE_REGEX: CENTRE_REGEX_BARCODE,
        CENTRE_KEY_BIOMEK_LABWARE_CLASS: BIOMEK_LABWARE_CLASS_BIO,
        CENTRE_KEY_FILE_NAMES_TO_IGNORE: ["CB_sanger_report_200714_0001_master.csv"],
        CENTRE_KEY_FILE_REGEX_CONSOLIDATED_EAGLE: r"^(EGC|CBE)\d+\.csv$",
        CENTRE_KEY_FILE_REGEX_CONSOLIDATED_SURVEILLANCE: r"^\d{9}\.csv$",
        CENTRE_KEY_FILE_REGEX_UNCONSOLIDATED_SURVEILLANCE: f"^CB_{CENTRE_REGEX_SFTP_FILE_HERON}",
        CENTRE_KEY_INCLUDE_IN_SCHEDULED_RUNS: True,
        CENTRE_KEY_LAB_ID_DEFAULT: "CB",
        CENTRE_KEY_NAME: "Cambridge-az",
        CENTRE_KEY_PREFIX: "CAMC",
        CENTRE_KEY_SFTP_ROOT_READ: "project-heron_cambridge-az",
        CENTRE_KEY_SKIP_UNCONSOLIDATED_SURVEILLANCE_FILES: False,
    },
    {
        CENTRE_KEY_BACKUPS_FOLDER: f"{CENTRE_DIR_BACKUPS}/RAND",
        CENTRE_KEY_BARCODE_FIELD: FIELD_RNA_ID,
        CENTRE_KEY_BARCODE_REGEX: CENTRE_REGEX_BARCODE,
        CENTRE_KEY_BIOMEK_LABWARE_CLASS: BIOMEK_LABWARE_CLASS_KINGFISHER,
        CENTRE_KEY_FILE_NAMES_TO_IGNORE: [r"^lw-randox-biocentre-box-((\d)|(1\d)|20)-.*$"],
        CENTRE_KEY_FILE_REGEX_CONSOLIDATED_EAGLE: r"^RXE\d+\.csv$",
        CENTRE_KEY_FILE_REGEX_CONSOLIDATED_SURVEILLANCE: r"^RDX-[a-zA-Z0-9]{2}-\d+\.csv$",
        CENTRE_KEY_FILE_REGEX_UNCONSOLIDATED_SURVEILLANCE: r"^lw-randox-biocentre-box-.*\.csv$",
        CENTRE_KEY_INCLUDE_IN_SCHEDULED_RUNS: True,
        CENTRE_KEY_LAB_ID_DEFAULT: "Randox",
        CENTRE_KEY_NAME: "Randox",
        CENTRE_KEY_PREFIX: "RAND",
        CENTRE_KEY_SFTP_ROOT_READ: "project-heron_randox",
        CENTRE_KEY_SKIP_UNCONSOLIDATED_SURVEILLANCE_FILES: False,
    },
    {
        CENTRE_KEY_BACKUPS_FOLDER: f"{CENTRE_DIR_BACKUPS}/HSL",
        CENTRE_KEY_BARCODE_FIELD: FIELD_RNA_ID,
        CENTRE_KEY_BARCODE_REGEX: CENTRE_REGEX_BARCODE,
        CENTRE_KEY_BIOMEK_LABWARE_CLASS: BIOMEK_LABWARE_CLASS_KINGFISHER,
        CENTRE_KEY_FILE_NAMES_TO_IGNORE: [],
        CENTRE_KEY_FILE_REGEX_CONSOLIDATED_EAGLE: r"^$",
        CENTRE_KEY_FILE_REGEX_CONSOLIDATED_SURVEILLANCE: r"^HSL\d+\.csv$",
        CENTRE_KEY_FILE_REGEX_UNCONSOLIDATED_SURVEILLANCE: r"^$",
        CENTRE_KEY_INCLUDE_IN_SCHEDULED_RUNS: True,
        CENTRE_KEY_LAB_ID_DEFAULT: "HSLL",
        CENTRE_KEY_NAME: "Health Services Laboratories",
        CENTRE_KEY_PREFIX: "HSLL",
        CENTRE_KEY_SFTP_ROOT_READ: "project-heron_hsl",
        CENTRE_KEY_SKIP_UNCONSOLIDATED_SURVEILLANCE_FILES: False,
    },
    {
        CENTRE_KEY_BACKUPS_FOLDER: f"{CENTRE_DIR_BACKUPS}/PLYM",
        CENTRE_KEY_BARCODE_FIELD: FIELD_RNA_ID,
        CENTRE_KEY_BARCODE_REGEX: CENTRE_REGEX_BARCODE,
        CENTRE_KEY_BIOMEK_LABWARE_CLASS: BIOMEK_LABWARE_CLASS_KINGFISHER,
        CENTRE_KEY_FILE_NAMES_TO_IGNORE: [],
        CENTRE_KEY_FILE_REGEX_CONSOLIDATED_EAGLE: r"^$",
        CENTRE_KEY_FILE_REGEX_CONSOLIDATED_SURVEILLANCE: r"^PLY-chp-\d+\.csv$",
        CENTRE_KEY_FILE_REGEX_UNCONSOLIDATED_SURVEILLANCE: f"^PLYM_{CENTRE_REGEX_SFTP_FILE_HERON}",
        CENTRE_KEY_INCLUDE_IN_SCHEDULED_RUNS: True,
        CENTRE_KEY_LAB_ID_DEFAULT: "PLYM",
        CENTRE_KEY_NAME: "Plymouth",
        CENTRE_KEY_PREFIX: "PLYM",
        CENTRE_KEY_SFTP_ROOT_READ: "project-heron_plym",
        CENTRE_KEY_SKIP_UNCONSOLIDATED_SURVEILLANCE_FILES: False,
    },
    {
        CENTRE_KEY_BACKUPS_FOLDER: f"{CENTRE_DIR_BACKUPS}/BRBR",
        CENTRE_KEY_BARCODE_FIELD: FIELD_RNA_ID,
        CENTRE_KEY_BARCODE_REGEX: CENTRE_REGEX_BARCODE,
        CENTRE_KEY_BIOMEK_LABWARE_CLASS: BIOMEK_LABWARE_CLASS_KINGFISHER,
        CENTRE_KEY_FILE_NAMES_TO_IGNORE: [],
        CENTRE_KEY_FILE_REGEX_CONSOLIDATED_EAGLE: r"^$",
        CENTRE_KEY_FILE_REGEX_CONSOLIDATED_SURVEILLANCE: r"^BB[-_]\d+\.csv$",
        CENTRE_KEY_FILE_REGEX_UNCONSOLIDATED_SURVEILLANCE: r"^$",
        CENTRE_KEY_INCLUDE_IN_SCHEDULED_RUNS: True,
        CENTRE_KEY_LAB_ID_DEFAULT: "BRBR",
        CENTRE_KEY_NAME: "Brants Bridge",
        CENTRE_KEY_PREFIX: "BRBR",
        CENTRE_KEY_SFTP_ROOT_READ: "project-heron_brbr",
        CENTRE_KEY_SKIP_UNCONSOLIDATED_SURVEILLANCE_FILES: False,
    },
    {
        CENTRE_KEY_BACKUPS_FOLDER: f"{CENTRE_DIR_BACKUPS}/LSPA",
        CENTRE_KEY_BARCODE_FIELD: FIELD_RNA_ID,
        CENTRE_KEY_BARCODE_REGEX: CENTRE_REGEX_BARCODE,
        CENTRE_KEY_BIOMEK_LABWARE_CLASS: BIOMEK_LABWARE_CLASS_KINGFISHER,
        CENTRE_KEY_FILE_NAMES_TO_IGNORE: [],
        CENTRE_KEY_FILE_REGEX_CONSOLIDATED_EAGLE: r"^$",
        CENTRE_KEY_FILE_REGEX_CONSOLIDATED_SURVEILLANCE: r"^LML_CHERY\d+\.csv$",
        CENTRE_KEY_FILE_REGEX_UNCONSOLIDATED_SURVEILLANCE: r"^$",
        CENTRE_KEY_INCLUDE_IN_SCHEDULED_RUNS: True,
        CENTRE_KEY_LAB_ID_DEFAULT: "LML",
        CENTRE_KEY_NAME: "Leamington Spa",
        CENTRE_KEY_PREFIX: "LSPA",
        CENTRE_KEY_SFTP_ROOT_READ: "project-heron_lspa",
        CENTRE_KEY_SKIP_UNCONSOLIDATED_SURVEILLANCE_FILES: False,
    },
    {
        CENTRE_KEY_BACKUPS_FOLDER: f"{CENTRE_DIR_BACKUPS}/NEWC",
        CENTRE_KEY_BARCODE_FIELD: FIELD_RNA_ID,
        CENTRE_KEY_BARCODE_REGEX: CENTRE_REGEX_BARCODE,
        CENTRE_KEY_BIOMEK_LABWARE_CLASS: BIOMEK_LABWARE_CLASS_KINGFISHER,
        CENTRE_KEY_FILE_NAMES_TO_IGNORE: [],
        CENTRE_KEY_FILE_REGEX_CONSOLIDATED_EAGLE: r"^$",
        CENTRE_KEY_FILE_REGEX_CONSOLIDATED_SURVEILLANCE: r"^ICHNE\d+[A-Z|a-z]{1}\.csv$",
        CENTRE_KEY_FILE_REGEX_UNCONSOLIDATED_SURVEILLANCE: r"^$",
        CENTRE_KEY_INCLUDE_IN_SCHEDULED_RUNS: True,
        CENTRE_KEY_LAB_ID_DEFAULT: "NCL",
        CENTRE_KEY_NAME: "Newcastle",
        CENTRE_KEY_PREFIX: "NEWC",
        CENTRE_KEY_SFTP_ROOT_READ: "project-heron_newc",
        CENTRE_KEY_SKIP_UNCONSOLIDATED_SURVEILLANCE_FILES: False,
    },
    {
        CENTRE_KEY_BACKUPS_FOLDER: f"{CENTRE_DIR_BACKUPS}/CPTD",
        CENTRE_KEY_BARCODE_FIELD: FIELD_RNA_ID,
        CENTRE_KEY_BARCODE_REGEX: CENTRE_REGEX_BARCODE,
        CENTRE_KEY_BIOMEK_LABWARE_CLASS: BIOMEK_LABWARE_CLASS_KINGFISHER,
        CENTRE_KEY_FILE_NAMES_TO_IGNORE: [],
        CENTRE_KEY_FILE_REGEX_CONSOLIDATED_EAGLE: r"^$",
        CENTRE_KEY_FILE_REGEX_CONSOLIDATED_SURVEILLANCE: r"^$",
        CENTRE_KEY_FILE_REGEX_UNCONSOLIDATED_SURVEILLANCE: r"^CPTD_\d{6}_\d{6}_\d{6}\.csv$",
        CENTRE_KEY_INCLUDE_IN_SCHEDULED_RUNS: False,
        CENTRE_KEY_LAB_ID_DEFAULT: "CPTD",
        CENTRE_KEY_NAME: "Cherrypick Test Data",
        CENTRE_KEY_PREFIX: TEST_DATA_CENTRE_PREFIX,
        CENTRE_KEY_SFTP_ROOT_READ: "",
        CENTRE_KEY_SKIP_UNCONSOLIDATED_SURVEILLANCE_FILES: False,
    },
]
