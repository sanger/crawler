# flake8: noqa
from decimal import Decimal
from typing import Final, Set, Tuple

###
# AP Scheduler Jobs
###
SCHEDULER_JOB_ID_RUN_CRAWLER: Final[str] = "run_crawler"

###
# CentreConf dictionary keys
###
CENTRE_KEY_BACKUPS_FOLDER: Final = "backups_folder"
CENTRE_KEY_BARCODE_FIELD: Final = "barcode_field"
CENTRE_KEY_BARCODE_REGEX: Final = "barcode_regex"
CENTRE_KEY_BIOMEK_LABWARE_CLASS: Final = "biomek_labware_class"
CENTRE_KEY_DATA_SOURCE: Final = "data_source"
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

###
# mongo collections
###
COLLECTION_CENTRES: Final[str] = "centres"
COLLECTION_IMPORTS: Final[str] = "imports"
COLLECTION_SAMPLES: Final[str] = "samples"
COLLECTION_PRIORITY_SAMPLES: Final[str] = "priority_samples"
COLLECTION_SOURCE_PLATES: Final[str] = "source_plates"
COLLECTION_CHERRYPICK_TEST_DATA: Final[str] = "cherrypick_test_data"

###
# CSV file column names
###
FIELD_ROOT_SAMPLE_ID: Final[str] = "Root Sample ID"
FIELD_RNA_ID: Final[str] = "RNA ID"
FIELD_VIRAL_PREP_ID: Final[str] = "Viral Prep ID"
FIELD_RNA_PCR_ID: Final[str] = "RNA-PCR ID"
FIELD_RESULT: Final[str] = "Result"
FIELD_DATE_TESTED: Final[str] = "Date Tested"
FIELD_LAB_ID: Final[str] = "Lab ID"
FIELD_CH1_TARGET: Final[str] = "CH1-Target"
FIELD_CH1_RESULT: Final[str] = "CH1-Result"
FIELD_CH1_CQ: Final[str] = "CH1-Cq"
FIELD_CH2_TARGET: Final[str] = "CH2-Target"
FIELD_CH2_RESULT: Final[str] = "CH2-Result"
FIELD_CH2_CQ: Final[str] = "CH2-Cq"
FIELD_CH3_TARGET: Final[str] = "CH3-Target"
FIELD_CH3_RESULT: Final[str] = "CH3-Result"
FIELD_CH3_CQ: Final[str] = "CH3-Cq"
FIELD_CH4_TARGET: Final[str] = "CH4-Target"
FIELD_CH4_RESULT: Final[str] = "CH4-Result"
FIELD_CH4_CQ: Final[str] = "CH4-Cq"
FIELD_TEST_KIT: Final[str] = "testKit"
FIELD_PICK_RESULT: Final[str] = "PickResult"

###
# mongo fields
###
FIELD_MONGODB_ID: Final[str] = "_id"
FIELD_PLATE_BARCODE: Final[str] = "plate_barcode"
FIELD_CENTRE_NAME: Final[str] = "name"
FIELD_COORDINATE: Final[str] = "coordinate"
FIELD_LINE_NUMBER: Final[str] = "line_number"
FIELD_FILE_NAME: Final[str] = "file_name"
FIELD_FILE_NAME_DATE: Final[str] = "file_name_date"
FIELD_CREATED_AT: Final[str] = "created_at"
FIELD_UPDATED_AT: Final[str] = "updated_at"
FIELD_SOURCE: Final[str] = "source"
FIELD_LH_SAMPLE_UUID: Final[str] = "lh_sample_uuid"
FIELD_LH_SOURCE_PLATE_UUID: Final[str] = "lh_source_plate_uuid"
FIELD_BARCODE: Final[str] = "barcode"
FIELD_MUST_SEQUENCE: Final[str] = "must_sequence"
FIELD_PREFERENTIALLY_SEQUENCE: Final[str] = "preferentially_sequence"
FIELD_PROCESSED: Final[str] = "processed"
FIELD_SAMPLE_ID: Final[str] = "sample_id"
FIELD_STATUS: Final[str] = "status"
FIELD_PLATE_SPECS: Final[str] = "plate_specs"
FIELD_ADD_TO_DART: Final[str] = "add_to_dart"
FIELD_BARCODES: Final[str] = "barcodes"
FIELD_FAILURE_REASON: Final[str] = "failure_reason"
FIELD_EVE_CREATED: Final[str] = "_created"
FIELD_EVE_UPDATED: Final[str] = "_updated"

# filtered-positive field names
FIELD_FILTERED_POSITIVE_TIMESTAMP: Final[str] = "filtered_positive_timestamp"
FIELD_FILTERED_POSITIVE_VERSION: Final[str] = "filtered_positive_version"
FIELD_FILTERED_POSITIVE: Final[str] = "filtered_positive"

# status field values
FIELD_STATUS_PENDING: Final[str] = "pending"
FIELD_STATUS_STARTED: Final[str] = "started"
FIELD_STATUS_PREPARING_DATA: Final[str] = "preparing_data"
FIELD_STATUS_CRAWLING_DATA: Final[str] = "crawling_data"
FIELD_STATUS_COMPLETED: Final[str] = "completed"
FIELD_STATUS_FAILED: Final[str] = "failed"

###
# cherrypicker test data
###
# the prefix for the centre which processes generated data
TEST_DATA_CENTRE_PREFIX: Final[str] = "CPTD"

# processing errors for the API endpoint for generating data
TEST_DATA_ERROR_NO_RUN_FOR_ID: Final[str] = "No run found for ID"
TEST_DATA_ERROR_WRONG_STATE: Final[str] = "Run doesn't have status"
TEST_DATA_ERROR_INVALID_PLATE_SPECS: Final[str] = "There is a problem with the plate specs for the run."
TEST_DATA_ERROR_NUMBER_OF_PLATES: Final[str] = "Number of plates to generate must be between 1 and {0}."
TEST_DATA_ERROR_NUMBER_OF_POS_SAMPLES: Final[
    str
] = "One or more plates expected fewer than 0 or more than 96 positive samples."
TEST_DATA_ERROR_BARACODA_COG_BARCODES: Final[str] = "Unable to create COG barcodes"
TEST_DATA_ERROR_BARACODA_CONNECTION: Final[str] = "Unable to access baracoda"
TEST_DATA_ERROR_BARACODA_UNKNOWN: Final[str] = "Unknown error accessing baracoda"


##
# multi-lims warehouse field names
###
MLWH_TABLE_NAME: Final[str] = "lighthouse_sample"
MLWH_MONGODB_ID: Final[str] = "mongodb_id"
MLWH_ROOT_SAMPLE_ID: Final[str] = "root_sample_id"
MLWH_RNA_ID: Final[str] = "rna_id"
MLWH_PLATE_BARCODE: Final[str] = "plate_barcode"
MLWH_COORDINATE: Final[str] = "coordinate"
MLWH_RESULT: Final[str] = "result"
MLWH_DATE_TESTED: Final[str] = "date_tested"
MLWH_SOURCE: Final[str] = "source"
MLWH_LAB_ID: Final[str] = "lab_id"
MLWH_CH1_TARGET: Final[str] = "ch1_target"
MLWH_CH1_RESULT: Final[str] = "ch1_result"
MLWH_CH1_CQ: Final[str] = "ch1_cq"
MLWH_CH2_TARGET: Final[str] = "ch2_target"
MLWH_CH2_RESULT: Final[str] = "ch2_result"
MLWH_CH2_CQ: Final[str] = "ch2_cq"
MLWH_CH3_TARGET: Final[str] = "ch3_target"
MLWH_CH3_RESULT: Final[str] = "ch3_result"
MLWH_CH3_CQ: Final[str] = "ch3_cq"
MLWH_CH4_TARGET: Final[str] = "ch4_target"
MLWH_CH4_RESULT: Final[str] = "ch4_result"
MLWH_CH4_CQ: Final[str] = "ch4_cq"
MLWH_FILTERED_POSITIVE: Final[str] = "filtered_positive"
MLWH_FILTERED_POSITIVE_VERSION: Final[str] = "filtered_positive_version"
MLWH_FILTERED_POSITIVE_TIMESTAMP: Final[str] = "filtered_positive_timestamp"
MLWH_LH_SAMPLE_UUID: Final[str] = "lh_sample_uuid"
MLWH_LH_SOURCE_PLATE_UUID: Final[str] = "lh_source_plate_uuid"
MLWH_CREATED_AT: Final[str] = "created_at"
MLWH_UPDATED_AT: Final[str] = "updated_at"
MLWH_MUST_SEQUENCE: Final[str] = "must_sequence"
MLWH_PREFERENTIALLY_SEQUENCE: Final[str] = "preferentially_sequence"
MLWH_IS_CURRENT: Final[str] = "is_current"

# datetime formats
MONGO_DATETIME_FORMAT: Final[str] = "%y%m%d_%H%M"

# 'Result' field value
RESULT_VALUE_POSITIVE: Final[str] = "Positive"
RESULT_VALUE_NEGATIVE: Final[str] = "Negative"
RESULT_VALUE_LIMIT_OF_DETECTION: Final[str] = "limit of detection"
RESULT_VALUE_VOID: Final[str] = "Void"

# allowed 'Result' field values
ALLOWED_RESULT_VALUES: Final[Tuple[str, str, str, str]] = (
    RESULT_VALUE_POSITIVE,
    RESULT_VALUE_NEGATIVE,
    RESULT_VALUE_LIMIT_OF_DETECTION,
    RESULT_VALUE_VOID,
)

# allowed CT channel CHn-Target field values (or can be null)
ALLOWED_CH_TARGET_VALUES: Final[Tuple[str, ...]] = (
    "E-Gene",
    "IEC",
    "MS2",
    "N gene",
    "ORF1ab",
    "RNaseP",
    "S gene",
)


CH_RESULT_INCONCLUSIVE = "Inconclusive"
CH_RESULT_NEGATIVE = "Negative"
CH_RESULT_POSITIVE = "Positive"
CH_RESULT_VOID = "Void"

# allowed CT channel CHn-Result field values (or can be null)
ALLOWED_CH_RESULT_VALUES: Final[Tuple[str, str, str, str]] = (
    CH_RESULT_INCONCLUSIVE,
    CH_RESULT_NEGATIVE,
    CH_RESULT_POSITIVE,
    CH_RESULT_VOID,
)

# range of allowed cq values (0 .. 100, set as strings for conversion to decimals in code)
MIN_CQ_VALUE: Final[Decimal] = Decimal("0.0")
MAX_CQ_VALUE: Final[Decimal] = Decimal("100.0")

###
# Ignored but understood headers
# These are headers we know about and can safely ignore; therefore, we do not need warnings for these
###
IGNORED_HEADERS: Final[Set[str]] = {FIELD_TEST_KIT, FIELD_PICK_RESULT}

###
# DART property names
###
DART_STATE: Final[str] = "state"
DART_ROOT_SAMPLE_ID: Final[str] = "root_sample_id"
DART_RNA_ID: Final[str] = "rna_id"
DART_LAB_ID: Final[str] = "lab_id"
DART_LH_SAMPLE_UUID: Final[str] = "lh_sample_uuid"

###
# DART property values
###
DART_STATE_PENDING: Final[str] = "pending"
DART_STATE_NO_PLATE: Final[str] = "NO PLATE"
DART_STATE_NO_PROP: Final[str] = "NO PROP"
DART_STATE_PICKABLE: Final[str] = "pickable"
DART_EMPTY_VALUE: Final[str] = ""

# DART others
DART_SET_PROP_STATUS_SUCCESS: Final[int] = 0

###
# Cut off date for v0 and v1 filtered positive
###
# Timestamp of v1 positive rule change (GPL-669) deployed to production
V0_V1_CUTOFF_TIMESTAMP: Final[str] = "2020-10-15 16:15:00"
# Timestamp of v2 positive rule change (Lighthouse deployment GPL-776)
V1_V2_CUTOFF_TIMESTAMP: Final[str] = "2020-12-15 14:19:03"

# Date on which filtered positive fields started being set by Crawler
FILTERED_POSITIVE_FIELDS_SET_DATE = "2020-12-17"

###
# Beckman labware
###
BIOMEK_LABWARE_CLASS_KINGFISHER: Final[str] = "KingFisher_96_2ml"
BIOMEK_LABWARE_CLASS_BIO: Final[str] = "Bio-Rad_96PCR"

# Sentinel workflow event to help determine sample cherrypicked status
EVENT_CHERRYPICK_LAYOUT_SET: Final[str] = "cherrypick_layout_set"

# As per Beckman events detailed in https://ssg-confluence.internal.sanger.ac.uk/display/PSDPUB/Cherrypicking+Events
PLATE_EVENT_DESTINATION_CREATED: Final[str] = "lh_beckman_cp_destination_created"

###
# RabbitMQ message keys
###
RABBITMQ_CREATE_FEEDBACK_ORIGIN_PARSING = "parsing"
RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE = "plate"
RABBITMQ_CREATE_FEEDBACK_ORIGIN_ROOT = "root"
RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE = "sample"

RABBITMQ_FIELD_LAB_ID = "labId"
RABBITMQ_FIELD_MESSAGE_UUID = "messageUuid"
RABBITMQ_FIELD_PLATE = "plate"

RABBITMQ_HEADER_KEY_SUBJECT = "subject"
RABBITMQ_HEADER_KEY_VERSION = "version"

RABBITMQ_ROUTING_KEY_CREATE_PLATE_FEEDBACK = "feedback.created.plate"

RABBITMQ_SUBJECT_CREATE_PLATE = "create-plate-map"
RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK = "create-plate-map-feedback"

###
# Flask endpoints
###

# general
FLASK_ERROR_UNEXPECTED: Final[str] = "An unexpected error occurred"
FLASK_ERROR_MISSING_PARAMETERS: Final[str] = "Missing required parameters"

# Set Download file age
FILE_AGE_IN_DAYS = 10
