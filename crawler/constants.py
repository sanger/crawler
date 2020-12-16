# flake8: noqa
from decimal import Decimal
import os

# mongo collections
COLLECTION_CENTRES = "centres"
COLLECTION_IMPORTS = "imports"
COLLECTION_SAMPLES = "samples"
COLLECTION_SAMPLES_HISTORY = "samples_history"
COLLECTION_SOURCE_PLATES = "source_plates"

# file column names
FIELD_ROOT_SAMPLE_ID = "Root Sample ID"
FIELD_RNA_ID = "RNA ID"
FIELD_VIRAL_PREP_ID = "Viral Prep ID"
FIELD_RNA_PCR_ID = "RNA-PCR ID"
FIELD_RESULT = "Result"
FIELD_DATE_TESTED = "Date Tested"
FIELD_LAB_ID = "Lab ID"
FIELD_CH1_TARGET = "CH1-Target"
FIELD_CH1_RESULT = "CH1-Result"
FIELD_CH1_CQ = "CH1-Cq"
FIELD_CH2_TARGET = "CH2-Target"
FIELD_CH2_RESULT = "CH2-Result"
FIELD_CH2_CQ = "CH2-Cq"
FIELD_CH3_TARGET = "CH3-Target"
FIELD_CH3_RESULT = "CH3-Result"
FIELD_CH3_CQ = "CH3-Cq"
FIELD_CH4_TARGET = "CH4-Target"
FIELD_CH4_RESULT = "CH4-Result"
FIELD_CH4_CQ = "CH4-Cq"

# other field names
FIELD_MONGODB_ID = "_id"
FIELD_PLATE_BARCODE = "plate_barcode"
FIELD_CENTRE_NAME = "name"
FIELD_COORDINATE = "coordinate"
FIELD_LINE_NUMBER = "line_number"
FIELD_FILE_NAME = "file_name"
FIELD_FILE_NAME_DATE = "file_name_date"
FIELD_CREATED_AT = "created_at"
FIELD_UPDATED_AT = "updated_at"
FIELD_SOURCE = "source"
FIELD_LH_SAMPLE_UUID = "lh_sample_uuid"
FIELD_LH_SOURCE_PLATE_UUID = "lh_source_plate_uuid"
FIELD_BARCODE = "barcode"

# filtered-positive field names
FIELD_FILTERED_POSITIVE_TIMESTAMP = "filtered_positive_timestamp"
FIELD_FILTERED_POSITIVE_VERSION = "filtered_positive_version"
FIELD_FILTERED_POSITIVE = "filtered_positive"

# multi-lims warehouse field names
MLWH_TABLE_NAME = "lighthouse_sample"
MLWH_MONGODB_ID = "mongodb_id"
MLWH_ROOT_SAMPLE_ID = "root_sample_id"
MLWH_RNA_ID = "rna_id"
MLWH_PLATE_BARCODE = "plate_barcode"
MLWH_COORDINATE = "coordinate"
MLWH_RESULT = "result"
MLWH_DATE_TESTED_STRING = "date_tested_string"
MLWH_DATE_TESTED = "date_tested"
MLWH_SOURCE = "source"
MLWH_LAB_ID = "lab_id"
MLWH_CH1_TARGET = "ch1_target"
MLWH_CH1_RESULT = "ch1_result"
MLWH_CH1_CQ = "ch1_cq"
MLWH_CH2_TARGET = "ch2_target"
MLWH_CH2_RESULT = "ch2_result"
MLWH_CH2_CQ = "ch2_cq"
MLWH_CH3_TARGET = "ch3_target"
MLWH_CH3_RESULT = "ch3_result"
MLWH_CH3_CQ = "ch3_cq"
MLWH_CH4_TARGET = "ch4_target"
MLWH_CH4_RESULT = "ch4_result"
MLWH_CH4_CQ = "ch4_cq"
MLWH_FILTERED_POSITIVE = "filtered_positive"
MLWH_FILTERED_POSITIVE_VERSION = "filtered_positive_version"
MLWH_FILTERED_POSITIVE_TIMESTAMP = "filtered_positive_timestamp"
MLWH_LH_SAMPLE_UUID = "lh_sample_uuid"
MLWH_LH_SOURCE_PLATE_UUID = "lh_source_plate_uuid"
MLWH_CREATED_AT = "created_at"
MLWH_UPDATED_AT = "updated_at"

# datetime formats
MONGO_DATETIME_FORMAT = "%y%m%d_%H%M"
MYSQL_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

# positive Result value
POSITIVE_RESULT_VALUE = "Positive"

# allowed Result field values
ALLOWED_RESULT_VALUES = (POSITIVE_RESULT_VALUE, "Negative", "limit of detection", "Void")

# allowed CT channel CHn-Target field values (or can be null)
ALLOWED_CH_TARGET_VALUES = ("ORF1ab", "N gene", "S gene", "MS2")

# allowed CT channel CHn-Result field values (or can be null)
ALLOWED_CH_RESULT_VALUES = (POSITIVE_RESULT_VALUE, "Negative", "Inconclusive", "Void")

# range of allowed cq values (0 .. 100, set as strings for conversion to decimals in code)
MIN_CQ_VALUE = Decimal("0.0")
MAX_CQ_VALUE = Decimal("100.0")

# DART property names
DART_STATE = "state"
DART_ROOT_SAMPLE_ID = "root_sample_id"
DART_RNA_ID = "rna_id"
DART_LAB_ID = "lab_id"
DART_LH_SAMPLE_UUID = "lh_sample_uuid"

# DART property values
DART_STATE_PENDING = "pending"
DART_STATE_NO_PLATE = "NO PLATE"
DART_STATE_NO_PROP = "NO PROP"
DART_STATE_PICKABLE = "pickable"
DART_EMPTY_VALUE = ""

# DART others
DART_SET_PROP_STATUS_SUCCESS = 0

# If we're running in a container, then instead of localhost
# we want host.docker.internal, you can specify this in the
# .env file you use for docker. eg
# LOCALHOST=host.docker.internal
LOCALHOST = os.environ.get("LOCALHOST", "127.0.0.1")
ROOT_PASSWORD=os.environ.get("ROOT_PASSWORD", "root")
