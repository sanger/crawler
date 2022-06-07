from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Union

import dateutil.parser
from bson.objectid import ObjectId

from crawler.constants import (
    EVENT_CHERRYPICK_LAYOUT_SET,
    FIELD_COORDINATE,
    FIELD_CREATED_AT,
    FIELD_FILTERED_POSITIVE,
    FIELD_FILTERED_POSITIVE_TIMESTAMP,
    FIELD_FILTERED_POSITIVE_VERSION,
    FIELD_MONGODB_ID,
    FIELD_MUST_SEQUENCE,
    FIELD_PLATE_BARCODE,
    FIELD_PREFERENTIALLY_SEQUENCE,
    FIELD_PROCESSED,
    FIELD_RESULT,
    FIELD_RNA_ID,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_SAMPLE_ID,
    FIELD_SOURCE,
    FILTERED_POSITIVE_FIELDS_SET_DATE,
    MLWH_CH1_CQ,
    MLWH_CH1_RESULT,
    MLWH_CH1_TARGET,
    MLWH_CH2_CQ,
    MLWH_CH2_RESULT,
    MLWH_CH2_TARGET,
    MLWH_CH3_CQ,
    MLWH_CH3_RESULT,
    MLWH_CH3_TARGET,
    MLWH_CH4_CQ,
    MLWH_CH4_RESULT,
    MLWH_CH4_TARGET,
    MLWH_COORDINATE,
    MLWH_CREATED_AT,
    MLWH_DATE_TESTED,
    MLWH_FILTERED_POSITIVE,
    MLWH_FILTERED_POSITIVE_TIMESTAMP,
    MLWH_FILTERED_POSITIVE_VERSION,
    MLWH_IS_CURRENT,
    MLWH_LAB_ID,
    MLWH_LH_SAMPLE_UUID,
    MLWH_LH_SOURCE_PLATE_UUID,
    MLWH_MONGODB_ID,
    MLWH_MUST_SEQUENCE,
    MLWH_PLATE_BARCODE,
    MLWH_PREFERENTIALLY_SEQUENCE,
    MLWH_RESULT,
    MLWH_RNA_ID,
    MLWH_ROOT_SAMPLE_ID,
    MLWH_SOURCE,
    MLWH_UPDATED_AT,
    PLATE_EVENT_DESTINATION_CREATED,
    RESULT_VALUE_NEGATIVE,
    RESULT_VALUE_POSITIVE,
    V0_V1_CUTOFF_TIMESTAMP,
    V1_V2_CUTOFF_TIMESTAMP,
)
from crawler.rabbit.messages.create_plate_message import FIELD_COG_UK_ID as CREATE_PLATE_COG_UK_ID
from crawler.rabbit.messages.create_plate_message import FIELD_FIT_TO_PICK as CREATE_PLATE_FIT_TO_PICK
from crawler.rabbit.messages.create_plate_message import FIELD_LAB_ID as CREATE_PLATE_LAB_ID
from crawler.rabbit.messages.create_plate_message import FIELD_MESSAGE_CREATE_DATE as CREATE_PLATE_MESSAGE_CREATE_DATE
from crawler.rabbit.messages.create_plate_message import FIELD_MESSAGE_UUID as CREATE_PLATE_MESSAGE_UUID
from crawler.rabbit.messages.create_plate_message import FIELD_MUST_SEQUENCE as CREATE_PLATE_MUST_SEQUENCE
from crawler.rabbit.messages.create_plate_message import FIELD_PLATE as CREATE_PLATE_PLATE
from crawler.rabbit.messages.create_plate_message import FIELD_PLATE_BARCODE as CREATE_PLATE_PLATE_BARCODE
from crawler.rabbit.messages.create_plate_message import FIELD_PLATE_COORDINATE as CREATE_PLATE_PLATE_COORDINATE
from crawler.rabbit.messages.create_plate_message import (
    FIELD_PREFERENTIALLY_SEQUENCE as CREATE_PLATE_PREFERENTIALLY_SEQUENCE,
)
from crawler.rabbit.messages.create_plate_message import FIELD_RESULT as CREATE_PLATE_RESULT
from crawler.rabbit.messages.create_plate_message import FIELD_RNA_ID as CREATE_PLATE_RNA_ID
from crawler.rabbit.messages.create_plate_message import FIELD_ROOT_SAMPLE_ID as CREATE_PLATE_ROOT_SAMPLE_ID
from crawler.rabbit.messages.create_plate_message import FIELD_SAMPLE_UUID as CREATE_PLATE_SAMPLE_UUID
from crawler.rabbit.messages.create_plate_message import FIELD_SAMPLES as CREATE_PLATE_SAMPLES
from crawler.rabbit.messages.create_plate_message import FIELD_TESTED_DATE as CREATE_PLATE_TESTED_DATE
from crawler.rabbit.messages.update_sample_message import FIELD_MESSAGE_CREATE_DATE as UPDATE_SAMPLE_MESSAGE_CREATE_DATE
from crawler.rabbit.messages.update_sample_message import FIELD_MESSAGE_UUID as UPDATE_SAMPLE_MESSAGE_UUID
from crawler.rabbit.messages.update_sample_message import FIELD_NAME as UPDATE_SAMPLE_NAME
from crawler.rabbit.messages.update_sample_message import FIELD_SAMPLE as UPDATE_SAMPLE_SAMPLE
from crawler.rabbit.messages.update_sample_message import FIELD_SAMPLE_UUID as UPDATE_SAMPLE_SAMPLE_UUID
from crawler.rabbit.messages.update_sample_message import FIELD_UPDATED_FIELDS as UPDATE_SAMPLE_UPDATED_FIELDS
from crawler.rabbit.messages.update_sample_message import FIELD_VALUE as UPDATE_SAMPLE_VALUE

TESTING_SAMPLES: List[Dict[str, Union[str, bool, ObjectId]]] = [
    {
        FIELD_COORDINATE: "A01",
        FIELD_SOURCE: "Test Centre",
        FIELD_RESULT: "Positive",
        FIELD_PLATE_BARCODE: "123",
        "released": True,
        FIELD_RNA_ID: "A01aaa",
        FIELD_ROOT_SAMPLE_ID: "MCM001",
        FIELD_MONGODB_ID: ObjectId("aaaaaaaaaaaaaaaaaaaaaaa1"),
    },
    {
        FIELD_COORDINATE: "B01",
        FIELD_SOURCE: "Test Centre",
        FIELD_RESULT: "Negative",
        FIELD_PLATE_BARCODE: "123",
        "released": False,
        FIELD_RNA_ID: "B01aaa",
        FIELD_ROOT_SAMPLE_ID: "MCM002",
        FIELD_MONGODB_ID: ObjectId("aaaaaaaaaaaaaaaaaaaaaaa2"),
    },
    {
        FIELD_COORDINATE: "C01",
        FIELD_SOURCE: "Test Centre",
        FIELD_RESULT: "Void",
        FIELD_PLATE_BARCODE: "123",
        FIELD_ROOT_SAMPLE_ID: "MCM003",
        FIELD_RNA_ID: "C01aaa",
        FIELD_MONGODB_ID: ObjectId("aaaaaaaaaaaaaaaaaaaaaaa3"),
    },
    {
        FIELD_COORDINATE: "D01",
        FIELD_SOURCE: "Test Centre",
        FIELD_RESULT: "Positive",
        FIELD_PLATE_BARCODE: "456",
        "released": True,
        FIELD_ROOT_SAMPLE_ID: "MCM004",
        FIELD_RNA_ID: "D01aaa",
        FIELD_MONGODB_ID: ObjectId("aaaaaaaaaaaaaaaaaaaaaaa4"),
    },
]

TESTING_PRIORITY_SAMPLES: List[Dict[str, Union[str, bool, ObjectId]]] = [
    {
        FIELD_SAMPLE_ID: ObjectId("aaaaaaaaaaaaaaaaaaaaaaa1"),
        FIELD_MUST_SEQUENCE: True,
        FIELD_PREFERENTIALLY_SEQUENCE: False,
        FIELD_PROCESSED: False,
    },
    {
        FIELD_SAMPLE_ID: ObjectId("aaaaaaaaaaaaaaaaaaaaaaa2"),
        FIELD_MUST_SEQUENCE: False,
        FIELD_PREFERENTIALLY_SEQUENCE: True,
        FIELD_PROCESSED: False,
    },
    {
        FIELD_SAMPLE_ID: ObjectId("aaaaaaaaaaaaaaaaaaaaaaa3"),
        FIELD_MUST_SEQUENCE: True,
        FIELD_PREFERENTIALLY_SEQUENCE: False,
        FIELD_PROCESSED: True,
    },
    {
        FIELD_SAMPLE_ID: ObjectId("aaaaaaaaaaaaaaaaaaaaaaa4"),
        FIELD_MUST_SEQUENCE: False,
        FIELD_PREFERENTIALLY_SEQUENCE: False,
        FIELD_PROCESSED: False,
    },
]


FILTERED_POSITIVE_TESTING_SAMPLES: List[Dict[str, Union[str, bool, datetime]]] = [
    {
        FIELD_COORDINATE: "A01",
        FIELD_SOURCE: "test1",
        FIELD_RESULT: "Positive",
        FIELD_PLATE_BARCODE: "123",
        "released": True,
        FIELD_ROOT_SAMPLE_ID: "MCM001",
        FIELD_FILTERED_POSITIVE: True,
        FIELD_FILTERED_POSITIVE_TIMESTAMP: "2020-01-01T00:00:00.000Z",
        FIELD_FILTERED_POSITIVE_VERSION: "v2",
        FIELD_CREATED_AT: dateutil.parser.parse(FILTERED_POSITIVE_FIELDS_SET_DATE) + timedelta(days=1),
    },
    {
        FIELD_COORDINATE: "B01",
        FIELD_SOURCE: "test1",
        FIELD_RESULT: "Positive",
        FIELD_PLATE_BARCODE: "123",
        "released": False,
        FIELD_ROOT_SAMPLE_ID: "MCM002",
        FIELD_FILTERED_POSITIVE: True,
        FIELD_FILTERED_POSITIVE_TIMESTAMP: "2020-01-01T00:00:00.000Z",
        FIELD_FILTERED_POSITIVE_VERSION: "v2",
        FIELD_CREATED_AT: dateutil.parser.parse(FILTERED_POSITIVE_FIELDS_SET_DATE) + timedelta(days=1),
    },
    {
        FIELD_COORDINATE: "C01",
        FIELD_SOURCE: "test1",
        FIELD_RESULT: "Negative",
        FIELD_PLATE_BARCODE: "123",
        FIELD_ROOT_SAMPLE_ID: "MCM003",
        FIELD_FILTERED_POSITIVE: False,
        FIELD_FILTERED_POSITIVE_TIMESTAMP: "2020-01-01T00:00:00.000Z",
        FIELD_FILTERED_POSITIVE_VERSION: "v2",
        FIELD_CREATED_AT: dateutil.parser.parse(FILTERED_POSITIVE_FIELDS_SET_DATE) + timedelta(days=1),
    },
    {
        FIELD_COORDINATE: "C01",
        FIELD_SOURCE: "test1",
        FIELD_RESULT: "Negative",
        FIELD_PLATE_BARCODE: "123",
        FIELD_ROOT_SAMPLE_ID: "MCM004",
        FIELD_FILTERED_POSITIVE: False,
        FIELD_FILTERED_POSITIVE_TIMESTAMP: "2020-01-01T00:00:00.000Z",
        FIELD_FILTERED_POSITIVE_VERSION: "v0",
        FIELD_CREATED_AT: dateutil.parser.parse(FILTERED_POSITIVE_FIELDS_SET_DATE) - timedelta(days=1),
    },
    {
        FIELD_COORDINATE: "D01",
        FIELD_SOURCE: "test1",
        FIELD_RESULT: RESULT_VALUE_POSITIVE,
        FIELD_PLATE_BARCODE: "123",
        FIELD_ROOT_SAMPLE_ID: "MCM005",
        FIELD_CREATED_AT: dateutil.parser.parse(FILTERED_POSITIVE_FIELDS_SET_DATE) - timedelta(days=1),
    },
    {
        FIELD_COORDINATE: "D01",
        FIELD_SOURCE: "test2",
        FIELD_RESULT: RESULT_VALUE_POSITIVE,
        FIELD_PLATE_BARCODE: "456",
        FIELD_ROOT_SAMPLE_ID: "MCM006",
        FIELD_CREATED_AT: dateutil.parser.parse(FILTERED_POSITIVE_FIELDS_SET_DATE) - timedelta(days=2),
    },
]

MONGO_SAMPLES_WITHOUT_FILTERED_POSITIVE_FIELDS: List[Dict[str, Union[str, bool]]] = [
    {
        FIELD_COORDINATE: "D01",
        FIELD_SOURCE: "test1",
        FIELD_RESULT: "Void",
        FIELD_PLATE_BARCODE: "123",
        FIELD_ROOT_SAMPLE_ID: "MCM004",
    },
    {
        FIELD_COORDINATE: "E01",
        FIELD_SOURCE: "test1",
        FIELD_RESULT: "Void",
        FIELD_PLATE_BARCODE: "456",
        FIELD_ROOT_SAMPLE_ID: "MCM005",
    },
    {
        FIELD_COORDINATE: "E01",
        FIELD_SOURCE: "test1",
        FIELD_RESULT: "Void",
        FIELD_PLATE_BARCODE: "456",
        FIELD_ROOT_SAMPLE_ID: "MCM006",
    },
    {
        FIELD_COORDINATE: "F01",
        FIELD_SOURCE: "test1",
        FIELD_RESULT: "Void",
        FIELD_PLATE_BARCODE: "456",
        FIELD_ROOT_SAMPLE_ID: "MCM007",
    },
]

MONGO_SAMPLES_WITH_FILTERED_POSITIVE_FIELDS = [
    {
        FIELD_MONGODB_ID: "1",
        FIELD_COORDINATE: "A01",
        FIELD_PLATE_BARCODE: "123",
        FIELD_ROOT_SAMPLE_ID: "MCM001",
        FIELD_RNA_ID: "AAA123",
        FIELD_FILTERED_POSITIVE: True,
        FIELD_FILTERED_POSITIVE_VERSION: "v2",
        FIELD_FILTERED_POSITIVE_TIMESTAMP: "2020-01-01T00:00:00.000Z",
    },
    {
        FIELD_MONGODB_ID: "2",
        FIELD_COORDINATE: "B01",
        FIELD_PLATE_BARCODE: "123",
        FIELD_ROOT_SAMPLE_ID: "MCM002",
        FIELD_RNA_ID: "BBB123",
        FIELD_FILTERED_POSITIVE: False,
        FIELD_FILTERED_POSITIVE_VERSION: "v2",
        FIELD_FILTERED_POSITIVE_TIMESTAMP: "2020-01-01T00:00:00.000Z",
    },
]

MLWH_SAMPLES_WITH_FILTERED_POSITIVE_FIELDS = [
    {
        MLWH_MONGODB_ID: "1",
        MLWH_COORDINATE: "A1",
        MLWH_PLATE_BARCODE: "123",
        MLWH_ROOT_SAMPLE_ID: "MCM001",
        MLWH_RNA_ID: "AAA123",
        MLWH_RESULT: RESULT_VALUE_POSITIVE,
        MLWH_FILTERED_POSITIVE: None,
        MLWH_FILTERED_POSITIVE_VERSION: None,
        MLWH_FILTERED_POSITIVE_TIMESTAMP: None,
    },
    {
        MLWH_MONGODB_ID: "2",
        MLWH_COORDINATE: "B1",
        MLWH_PLATE_BARCODE: "123",
        MLWH_ROOT_SAMPLE_ID: "MCM002",
        MLWH_RNA_ID: "BBB123",
        MLWH_RESULT: RESULT_VALUE_POSITIVE,
        MLWH_FILTERED_POSITIVE: True,
        MLWH_FILTERED_POSITIVE_VERSION: "v1.0",
        MLWH_FILTERED_POSITIVE_TIMESTAMP: datetime(2020, 4, 23, 14, 40, 8),
    },
]

EVENT_WH_DATA: Dict[str, Any] = {
    "subjects": [
        {"id": 1, "uuid": "1".encode("utf-8"), "friendly_name": "ss1", "subject_type_id": 1},
        {"id": 2, "uuid": "2".encode("utf-8"), "friendly_name": "ss2", "subject_type_id": 1},
        {"id": 3, "uuid": "3".encode("utf-8"), "friendly_name": "ss3", "subject_type_id": 1},
        {"id": 4, "uuid": "6".encode("utf-8"), "friendly_name": "ss1-beck", "subject_type_id": 1},
        {"id": 5, "uuid": "7".encode("utf-8"), "friendly_name": "ss2-beck", "subject_type_id": 1},
    ],
    "roles": [
        {"id": 1, "event_id": 1, "subject_id": 1, "role_type_id": 1},
        {"id": 2, "event_id": 2, "subject_id": 2, "role_type_id": 1},
        {"id": 3, "event_id": 3, "subject_id": 3, "role_type_id": 1},
        {"id": 4, "event_id": 5, "subject_id": 4, "role_type_id": 1},
        {"id": 5, "event_id": 6, "subject_id": 5, "role_type_id": 1},
    ],
    "events": [
        {
            "id": 1,
            "lims_id": "SQSCP",
            "uuid": "1".encode("utf-8"),
            "event_type_id": 1,
            "occured_at": "2020-09-25 11:35:30",
            "user_identifier": "test@example.com",
        },
        {
            "id": 2,
            "lims_id": "SQSCP",
            "uuid": "2".encode("utf-8"),
            "event_type_id": 1,
            "occured_at": "2020-10-25 11:35:30",
            "user_identifier": "test@example.com",
        },
        {
            "id": 3,
            "lims_id": "SQSCP",
            "uuid": "3".encode("utf-8"),
            "event_type_id": 1,
            "occured_at": "2020-10-15 16:35:30",
            "user_identifier": "test@example.com",
        },
        {
            "id": 4,
            "lims_id": "SQSCP",
            "uuid": "4".encode("utf-8"),
            "event_type_id": 1,
            "occured_at": "2020-10-15 16:35:30",
            "user_identifier": "test@example.com",
        },
        {
            "id": 5,
            "lims_id": "SQSCP",
            "uuid": "5".encode("utf-8"),
            "event_type_id": 2,
            "occured_at": "2020-10-15 16:35:30",
            "user_identifier": "test@example.com",
        },
        {
            "id": 6,
            "lims_id": "SQSCP",
            "uuid": "6".encode("utf-8"),
            "event_type_id": 2,
            "occured_at": "2020-10-15 16:35:30",
            "user_identifier": "test@example.com",
        },
    ],
    "event_types": [
        {"id": 1, "key": EVENT_CHERRYPICK_LAYOUT_SET, "description": "stuff"},
        {"id": 2, "key": PLATE_EVENT_DESTINATION_CREATED, "description": "stuff"},
    ],
    "subject_types": [{"id": 1, "key": "sample", "description": "stuff"}],
    "role_types": [{"id": 1, "key": "sample", "description": "stuff"}],
}

MLWH_SAMPLE_STOCK_RESOURCE: Dict[str, Any] = {
    "sample": [
        {
            "id_sample_tmp": "1",
            "id_sample_lims": "1",
            "description": "root_1",
            "supplier_name": "cog_uk_id_1",
            "phenotype": "positive",
            "sanger_sample_id": "ss1",
            "id_lims": "SQSCP",
            "last_updated": "2015-11-25 11:35:30",
            "recorded_at": "2015-11-25 11:35:30",
            "created": str(
                datetime.strptime(V0_V1_CUTOFF_TIMESTAMP, "%Y-%m-%d %H:%M:%S")
            ),  # Created at v0/v1 cut-off time
            "uuid_sample_lims": "1",
        },
        {
            "id_sample_tmp": "2",
            "id_sample_lims": "2",
            "description": "root_2",
            "supplier_name": "cog_uk_id_2",
            "phenotype": "positive",
            "sanger_sample_id": "ss2",
            "id_lims": "SQSCP",
            "last_updated": "2015-11-25 11:35:30",
            "recorded_at": "2015-11-25 11:35:30",
            "created": str(
                datetime.strptime(V0_V1_CUTOFF_TIMESTAMP, "%Y-%m-%d %H:%M:%S") - timedelta(days=1)
            ),  # Created before v0/v1 cut-off time
            "uuid_sample_lims": "2",
        },
        {
            "id_sample_tmp": "3",
            "id_sample_lims": "3",
            "description": "root_3",
            "supplier_name": "cog_uk_id_3",
            "phenotype": "positive",
            "sanger_sample_id": "ss3",
            "id_lims": "SQSCP",
            "last_updated": "2015-11-25 11:35:30",
            "recorded_at": "2015-11-25 11:35:30",
            "created": str(
                datetime.strptime(V0_V1_CUTOFF_TIMESTAMP, "%Y-%m-%d %H:%M:%S") + timedelta(days=1)
            ),  # Created after v0/v1 cut-off time
            "uuid_sample_lims": "3",
        },
        {
            "id_sample_tmp": "4",
            "id_sample_lims": "4",
            "description": "root_4",
            "supplier_name": "cog_uk_id_4",
            "phenotype": "positive",
            "sanger_sample_id": "ss4",
            "id_lims": "SQSCP",
            "last_updated": "2015-11-25 11:35:30",
            "recorded_at": "2015-11-25 11:35:30",
            "created": str(
                datetime.strptime(V1_V2_CUTOFF_TIMESTAMP, "%Y-%m-%d %H:%M:%S") + timedelta(days=1)
            ),  # Created after v1/v2 cut-off time
            "uuid_sample_lims": "4",
        },
        {
            "id_sample_tmp": "5",
            "id_sample_lims": "5",
            "description": "root_1",
            "supplier_name": "cog_uk_id_5",
            "phenotype": "positive",
            "sanger_sample_id": "ss5",
            "id_lims": "SQSCP",
            "last_updated": "2015-11-25 11:35:30",
            "recorded_at": "2015-11-25 11:35:30",
            "created": str(
                datetime.strptime(FILTERED_POSITIVE_FIELDS_SET_DATE, "%Y-%m-%d") + timedelta(days=1)
            ),  # Created after filtered positive fields set
            "uuid_sample_lims": "5",
        },
    ],
    "stock_resource": [
        {
            "id_stock_resource_tmp": "1",
            "id_sample_tmp": "1",
            "labware_human_barcode": "pb_1",
            "labware_machine_barcode": "pb_1",
            "labware_coordinate": "A1",
            "last_updated": "2015-11-25 11:35:30",
            "recorded_at": "2015-11-25 11:35:30",
            "created": "2015-11-25 11:35:30",
            "id_study_tmp": "1",
            "id_lims": "SQSCP",
            "id_stock_resource_lims": "1",
            "labware_type": "well",
        },
        {
            "id_stock_resource_tmp": "2",
            "id_sample_tmp": "2",
            "labware_human_barcode": "pb_2",
            "labware_machine_barcode": "pb_2",
            "labware_coordinate": "A1",
            "last_updated": "2015-11-25 11:35:30",
            "recorded_at": "2015-11-25 11:35:30",
            "created": "2015-11-25 11:35:30",
            "id_study_tmp": "1",
            "id_lims": "SQSCP",
            "id_stock_resource_lims": "2",
            "labware_type": "well",
        },
        {
            "id_stock_resource_tmp": "3",
            "id_sample_tmp": "3",
            "labware_human_barcode": "pb_3",
            "labware_machine_barcode": "pb_3",
            "labware_coordinate": "A1",
            "last_updated": "2015-11-25 11:35:30",
            "recorded_at": "2015-11-25 11:35:30",
            "created": "2015-11-25 11:35:30",
            "id_study_tmp": "1",
            "id_lims": "SQSCP",
            "id_stock_resource_lims": "3",
            "labware_type": "well",
        },
        {
            "id_stock_resource_tmp": "4",
            "id_sample_tmp": "4",
            "labware_human_barcode": "pb_3",
            "labware_machine_barcode": "pb_3",
            "labware_coordinate": "A1",
            "last_updated": "2015-11-25 11:35:30",
            "recorded_at": "2015-11-25 11:35:30",
            "created": "2015-11-25 11:35:30",
            "id_study_tmp": "1",
            "id_lims": "SQSCP",
            "id_stock_resource_lims": "4",
            "labware_type": "well",
        },
        {
            "id_stock_resource_tmp": "5",
            "id_sample_tmp": "5",
            "labware_human_barcode": "pb_3",
            "labware_machine_barcode": "pb_3",
            "labware_coordinate": "A1",
            "last_updated": "2015-11-25 11:35:30",
            "recorded_at": "2015-11-25 11:35:30",
            "created": "2015-11-25 11:35:30",
            "id_study_tmp": "1",
            "id_lims": "SQSCP",
            "id_stock_resource_lims": "5",
            "labware_type": "well",
        },
    ],
    "study": [
        {
            "id_study_tmp": "1",
            "last_updated": "2015-11-25 11:35:30",
            "recorded_at": "2015-11-25 11:35:30",
            "id_study_lims": "1",
            "id_lims": "SQSCP",
        }
    ],
}


MLWH_SAMPLE_LIGHTHOUSE_SAMPLE: Dict[str, Any] = {
    "sample": [
        {
            "id_sample_tmp": "6",
            "id_sample_lims": "6",
            "description": "root_5",
            "supplier_name": "cog_uk_id_6",
            "phenotype": "positive",
            "sanger_sample_id": "beck-ss1",
            "id_lims": "SQSCP",
            "last_updated": "2015-11-25 11:35:30",
            "recorded_at": "2015-11-25 11:35:30",
            "created": "2015-11-25 11:35:30",
            "uuid_sample_lims": "36000000000000000000000000000000",
        },
        {
            "id_sample_tmp": "7",
            "id_sample_lims": "7",
            "description": "root_6",
            "supplier_name": "cog_uk_id_7",
            "phenotype": "positive",
            "sanger_sample_id": "beck-ss2",
            "id_lims": "SQSCP",
            "last_updated": "2015-11-25 11:35:30",
            "recorded_at": "2015-11-25 11:35:30",
            "created": "2015-11-25 11:35:30",
            "uuid_sample_lims": "37000000000000000000000000000000",
        },
        {
            "id_sample_tmp": "8",
            "id_sample_lims": "8",
            "description": "root_5",
            "supplier_name": "cog_uk_id_8",
            "phenotype": "positive",
            "sanger_sample_id": "beck-ss3",
            "id_lims": "SQSCP",
            "last_updated": "2015-11-25 11:35:30",
            "recorded_at": "2015-11-25 11:35:30",
            "created": "2015-11-25 11:35:30",
            "uuid_sample_lims": "38000000000000000000000000000000",
        },
        {
            "id_sample_tmp": "9",
            "id_sample_lims": "9",
            "description": "root_4",
            "supplier_name": "cog_uk_id_9",
            "phenotype": "positive",
            "sanger_sample_id": "beck-ss4",
            "id_lims": "SQSCP",
            "last_updated": "2015-11-25 11:35:30",
            "recorded_at": "2015-11-25 11:35:30",
            "created": "2015-11-25 11:35:30",
            "uuid_sample_lims": "39000000000000000000000000000000",
        },
    ],
    "lighthouse_sample": [
        {
            "mongodb_id": "1",
            "root_sample_id": "root_5",
            "rna_id": "pb_4_A01",
            "plate_barcode": "pb_4",
            "coordinate": "A1",
            "result": "Positive",
            "date_tested_string": "2020-10-24 22:30:22",
            "date_tested": datetime(2020, 10, 24, 22, 30, 22),
            "source": "test centre",
            "lab_id": "TC",
            "lh_sample_uuid": "36000000000000000000000000000000",
        },
        {
            "mongodb_id": "2",
            "root_sample_id": "root_6",
            "rna_id": "pb_5_A01",
            "plate_barcode": "pb_5",
            "coordinate": "A1",
            "result": "Positive",
            "date_tested_string": "2020-10-24 22:30:22",
            "date_tested": datetime(2020, 10, 24, 22, 30, 22),
            "source": "test centre",
            "lab_id": "TC",
            "lh_sample_uuid": "37000000000000000000000000000000",
        },
        {
            "mongodb_id": "3",
            "root_sample_id": "root_5",
            "rna_id": "pb_6_A01",
            "plate_barcode": "pb_6",
            "coordinate": "A1",
            "result": "Positive",
            "date_tested_string": "2020-10-24 22:30:22",
            "date_tested": datetime(2020, 10, 24, 22, 30, 22),
            "source": "test centre",
            "lab_id": "TC",
            "lh_sample_uuid": "38000000000000000000000000000000",
        },
        {
            "mongodb_id": "4",
            "root_sample_id": "root_4",
            "rna_id": "pb_3_A01",
            "plate_barcode": "pb_3",
            "coordinate": "A1",
            "result": "Positive",
            "date_tested_string": "2020-10-24 22:30:22",
            "date_tested": datetime(2020, 10, 24, 22, 30, 22),
            "source": "test centre",
            "lab_id": "TC",
            "lh_sample_uuid": "39000000000000000000000000000000",
        },
    ],
}


MLWH_SAMPLE_COMPLETE = {
    MLWH_CH1_CQ: Decimal("24.67"),
    MLWH_CH1_RESULT: RESULT_VALUE_POSITIVE,
    MLWH_CH1_TARGET: "A gene",
    MLWH_CH2_CQ: Decimal("23.92"),
    MLWH_CH2_RESULT: RESULT_VALUE_NEGATIVE,
    MLWH_CH2_TARGET: "B gene",
    MLWH_CH3_CQ: Decimal("25.12"),
    MLWH_CH3_RESULT: RESULT_VALUE_POSITIVE,
    MLWH_CH3_TARGET: "C gene",
    MLWH_CH4_CQ: Decimal("22.86"),
    MLWH_CH4_RESULT: RESULT_VALUE_NEGATIVE,
    MLWH_CH4_TARGET: "D gene",
    MLWH_COORDINATE: "C3",
    MLWH_DATE_TESTED: "2021-02-03 04:05:06",
    MLWH_FILTERED_POSITIVE: True,
    MLWH_FILTERED_POSITIVE_TIMESTAMP: datetime(2021, 2, 3, 5, 6, 7),
    MLWH_FILTERED_POSITIVE_VERSION: "v3",
    MLWH_IS_CURRENT: True,
    MLWH_LAB_ID: "BB",
    MLWH_LH_SAMPLE_UUID: "233223d5-9015-4646-add0-f358ff2688c7",
    MLWH_LH_SOURCE_PLATE_UUID: "c6410270-5cbf-4233-a8d1-b08445bbac5e",
    MLWH_MONGODB_ID: "6140f388800f8fe309689124",
    MLWH_MUST_SEQUENCE: True,
    MLWH_PLATE_BARCODE: "95123456789012345",
    MLWH_PREFERENTIALLY_SEQUENCE: False,
    MLWH_RESULT: RESULT_VALUE_POSITIVE,
    MLWH_RNA_ID: "95123456789012345_C03",
    MLWH_ROOT_SAMPLE_ID: "BAA94123456",
    MLWH_SOURCE: "Bob's Biotech",
    MLWH_CREATED_AT: datetime.utcnow(),
    MLWH_UPDATED_AT: datetime.utcnow(),
}


CREATE_PLATE_MESSAGE = {
    CREATE_PLATE_MESSAGE_UUID: b"CREATE_PLATE_UUID",
    CREATE_PLATE_MESSAGE_CREATE_DATE: datetime.utcnow(),
    CREATE_PLATE_PLATE: {
        CREATE_PLATE_LAB_ID: "CPTD",
        CREATE_PLATE_PLATE_BARCODE: "PLATE-001",
        CREATE_PLATE_SAMPLES: [
            {
                CREATE_PLATE_SAMPLE_UUID: b"UUID_001",
                CREATE_PLATE_ROOT_SAMPLE_ID: "R00T-S4MPL3-ID1",
                CREATE_PLATE_RNA_ID: "RN4-1D-1",
                CREATE_PLATE_COG_UK_ID: "C0G-UK-ID-1",
                CREATE_PLATE_PLATE_COORDINATE: "A1",
                CREATE_PLATE_PREFERENTIALLY_SEQUENCE: False,
                CREATE_PLATE_MUST_SEQUENCE: False,
                CREATE_PLATE_FIT_TO_PICK: True,
                CREATE_PLATE_RESULT: "positive",
                CREATE_PLATE_TESTED_DATE: datetime(2022, 4, 10, 11, 45, 25),
            },
            {
                CREATE_PLATE_SAMPLE_UUID: b"UUID_002",
                CREATE_PLATE_ROOT_SAMPLE_ID: "R00T-S4MPL3-ID2",
                CREATE_PLATE_RNA_ID: "RN4-1D-2",
                CREATE_PLATE_PLATE_COORDINATE: "E6",
                CREATE_PLATE_COG_UK_ID: "C0G-UK-ID-2",
                CREATE_PLATE_PREFERENTIALLY_SEQUENCE: False,
                CREATE_PLATE_MUST_SEQUENCE: True,
                CREATE_PLATE_FIT_TO_PICK: False,
                CREATE_PLATE_RESULT: "negative",
                CREATE_PLATE_TESTED_DATE: datetime(2022, 4, 10, 11, 45, 25),
            },
            {
                CREATE_PLATE_SAMPLE_UUID: b"UUID_003",
                CREATE_PLATE_ROOT_SAMPLE_ID: "R00T-S4MPL3-ID3",
                CREATE_PLATE_RNA_ID: "RN4-1D-3",
                CREATE_PLATE_PLATE_COORDINATE: "H12",
                CREATE_PLATE_COG_UK_ID: "C0G-UK-ID-3",
                CREATE_PLATE_PREFERENTIALLY_SEQUENCE: True,
                CREATE_PLATE_MUST_SEQUENCE: True,
                CREATE_PLATE_FIT_TO_PICK: True,
                CREATE_PLATE_RESULT: "void",
                CREATE_PLATE_TESTED_DATE: datetime(2022, 4, 10, 11, 45, 25),
            },
        ],
    },
}


UPDATE_SAMPLE_MESSAGE = {
    UPDATE_SAMPLE_MESSAGE_UUID: b"UPDATE_SAMPLE_MESSAGE_UUID",
    UPDATE_SAMPLE_MESSAGE_CREATE_DATE: datetime.utcnow(),
    UPDATE_SAMPLE_SAMPLE: {
        UPDATE_SAMPLE_SAMPLE_UUID: b"UPDATE_SAMPLE_UUID",
        UPDATE_SAMPLE_UPDATED_FIELDS: [
            {
                UPDATE_SAMPLE_NAME: "mustSequence",
                UPDATE_SAMPLE_VALUE: True,
            },
            {
                UPDATE_SAMPLE_NAME: "preferentiallySequence",
                UPDATE_SAMPLE_VALUE: True,
            },
        ],
    },
}
