# flake8: noqa
from decimal import Decimal
from typing import Final, Tuple

# mongo collections
COLLECTION_CENTRES: Final[str] = "centres"
COLLECTION_IMPORTS: Final[str] = "imports"
COLLECTION_SAMPLES: Final[str] = "samples"
COLLECTION_SAMPLES_HISTORY: Final[str] = "samples_history"
COLLECTION_SOURCE_PLATES: Final[str] = "source_plates"

# file column names
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

# other field names
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

# filtered-positive field names
FIELD_FILTERED_POSITIVE_TIMESTAMP: Final[str] = "filtered_positive_timestamp"
FIELD_FILTERED_POSITIVE_VERSION: Final[str] = "filtered_positive_version"
FIELD_FILTERED_POSITIVE: Final[str] = "filtered_positive"

# multi-lims warehouse field names
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

# datetime formats
MONGO_DATETIME_FORMAT: Final[str] = "%y%m%d_%H%M"

# positive Result value
POSITIVE_RESULT_VALUE: Final[str] = "Positive"
LIMIT_OF_DETECTION_RESULT_VALUE: Final[str] = "limit of detection"

# allowed Result field values
ALLOWED_RESULT_VALUES: Final[Tuple[str, str, str, str]] = (
    POSITIVE_RESULT_VALUE,
    "Negative",
    LIMIT_OF_DETECTION_RESULT_VALUE,
    "Void",
)

# allowed CT channel CHn-Target field values (or can be null)
ALLOWED_CH_TARGET_VALUES: Final[Tuple[str, ...]] = (
    "E-Gene",
    "IEC",
    "MS2",
    "N gene",
    "ORF1ab",
    "S gene",
)

# allowed CT channel CHn-Result field values (or can be null)
ALLOWED_CH_RESULT_VALUES: Final[Tuple[str, str, str, str]] = (POSITIVE_RESULT_VALUE, "Negative", "Inconclusive", "Void")

# range of allowed cq values (0 .. 100, set as strings for conversion to decimals in code)
MIN_CQ_VALUE: Final[Decimal] = Decimal("0.0")
MAX_CQ_VALUE: Final[Decimal] = Decimal("100.0")

# DART property names
DART_STATE: Final[str] = "state"
DART_ROOT_SAMPLE_ID: Final[str] = "root_sample_id"
DART_RNA_ID: Final[str] = "rna_id"
DART_LAB_ID: Final[str] = "lab_id"
DART_LH_SAMPLE_UUID: Final[str] = "lh_sample_uuid"

# DART property values
DART_STATE_PENDING: Final[str] = "pending"
DART_STATE_NO_PLATE: Final[str] = "NO PLATE"
DART_STATE_NO_PROP: Final[str] = "NO PROP"
DART_STATE_PICKABLE: Final[str] = "pickable"
DART_EMPTY_VALUE: Final[str] = ""

# DART others
DART_SET_PROP_STATUS_SUCCESS: Final[int] = 0

# Cut off date for v0 and v1 filtered positive
# Timestamp of v1 positive rule change (GPL-669) deployed to production
V0_V1_CUTOFF_TIMESTAMP: Final[str] = "2020-10-15 16:15:00"
# Timestamp of v2 positive rule change (Lighthouse deployment GPL-776)
V1_V2_CUTOFF_TIMESTAMP: Final[str] = "2020-12-15 14:19:03"

# Date on which filtered positive fields started being set by Crawler
FILTERED_POSITIVE_FIELDS_SET_DATE = "2020-12-17"

# Beckman labware
BIOMEK_LABWARE_CLASS_KINGFISHER: Final[str] = "KingFisher_96_2ml"
BIOMEK_LABWARE_CLASS_BIO: Final[str] = "Bio-Rad_96PCR"

# Sentinel workflow event to help determine sample cherrypicked status
EVENT_CHERRYPICK_LAYOUT_SET: Final[str] = "cherrypick_layout_set"

# As per Beckman events detailed in https://ssg-confluence.internal.sanger.ac.uk/display/PSDPUB/Cherrypicking+Events
PLATE_EVENT_DESTINATION_CREATED: Final[str] = "lh_beckman_cp_destination_created"
