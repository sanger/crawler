import logging
import copy
import logging.config
import shutil
from typing import Dict, List, Union, Any
from unittest.mock import patch
import sqlalchemy  # type: ignore
from sqlalchemy.engine.base import Engine  # type: ignore
from sqlalchemy import MetaData  # type: ignore
from datetime import datetime, timedelta
import dateutil.parser

import pytest
from crawler.constants import (
    COLLECTION_CENTRES,
    COLLECTION_SAMPLES,
    COLLECTION_SAMPLES_HISTORY,
    FIELD_MONGODB_ID,
    FIELD_COORDINATE,
    FIELD_PLATE_BARCODE,
    FIELD_RESULT,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_SOURCE,
    FIELD_RNA_ID,
    MLWH_TABLE_NAME,
    FIELD_FILTERED_POSITIVE,
    FIELD_FILTERED_POSITIVE_TIMESTAMP,
    FIELD_FILTERED_POSITIVE_VERSION,
    FIELD_CREATED_AT,
    V0_V1_CUTOFF_TIMESTAMP,
    V1_V2_CUTOFF_TIMESTAMP,
    FILTERED_POSITIVE_FIELDS_SET_DATE,
    POSITIVE_RESULT_VALUE,
    MLWH_MONGODB_ID,
    MLWH_COORDINATE,
    MLWH_PLATE_BARCODE,
    MLWH_ROOT_SAMPLE_ID,
    MLWH_RNA_ID,
    MLWH_RESULT,
    MLWH_FILTERED_POSITIVE,
    MLWH_FILTERED_POSITIVE_VERSION,
    MLWH_FILTERED_POSITIVE_TIMESTAMP,
    EVENT_CHERRYPICK_LAYOUT_SET,
    PLATE_EVENT_DESTINATION_CREATED,
)
from crawler.db import create_mongo_client, create_mysql_connection, get_mongo_collection, get_mongo_db
from crawler.helpers.general_helpers import get_config

logger = logging.getLogger(__name__)
CONFIG, _ = get_config("crawler.config.test")
logging.config.dictConfig(CONFIG.LOGGING)  # type: ignore


@pytest.fixture
def config():
    return CONFIG


@pytest.fixture
def mongo_client(config):
    with create_mongo_client(config) as client:
        yield config, client


@pytest.fixture
def mongo_database(mongo_client):
    config, mongo_client = mongo_client
    db = get_mongo_db(config, mongo_client)
    try:
        yield config, db
    # Drop the database after each test to ensure they are independent
    # A transaction may be more appropriate here, but that means significant
    # code changes, as 'sessions' need to be passed around. I'm also not
    # sure what version of mongo is being used in production.
    finally:
        mongo_client.drop_database(db)


@pytest.fixture
def mlwh_connection(config):
    mysql_conn = create_mysql_connection(config, readonly=False)
    try:
        cursor = mysql_conn.cursor()
        # clear any existing rows in the lighthouse sample table
        try:
            cursor.execute(f"TRUNCATE TABLE {config.MLWH_DB_DBNAME}.{MLWH_TABLE_NAME}")
            mysql_conn.commit()
        except Exception:
            pytest.fail("An exception occurred clearing the table")
        finally:
            cursor.close()
        yield mysql_conn
    finally:
        # close the connection
        mysql_conn.close()


@pytest.fixture
def pyodbc_conn(config):
    with patch("pyodbc.connect") as mock_connect:
        yield mock_connect


@pytest.fixture
def testing_files_for_process(cleanup_backups):
    """Copy the test files to a new directory, as we expect run() to perform a clean up, and we don't want it cleaning
    up our main copy of the data.

    We don't disable the clean up as:
    1. It also clears up the master files, which we'd otherwise need to handle
    TODO: remove reference to master files above - they don't exist anymore
    2. It means we keep the tested process closer to the actual one
    """
    _ = shutil.copytree("tests/files", "tmp/files", dirs_exist_ok=True)
    try:
        yield
    finally:
        # remove files https://docs.python.org/3/library/shutil.html#shutil.rmtree
        shutil.rmtree("tmp/files")


TESTING_SAMPLES: List[Dict[str, Union[str, bool]]] = [
    {
        FIELD_COORDINATE: "A01",
        FIELD_SOURCE: "test1",
        FIELD_RESULT: "Positive",
        FIELD_PLATE_BARCODE: "123",
        "released": True,
        FIELD_ROOT_SAMPLE_ID: "MCM001",
    },
    {
        FIELD_COORDINATE: "B01",
        FIELD_SOURCE: "test1",
        FIELD_RESULT: "Negative",
        FIELD_PLATE_BARCODE: "123",
        "released": False,
        FIELD_ROOT_SAMPLE_ID: "MCM002",
    },
    {
        FIELD_COORDINATE: "C01",
        FIELD_SOURCE: "test1",
        FIELD_RESULT: "Void",
        FIELD_PLATE_BARCODE: "123",
        FIELD_ROOT_SAMPLE_ID: "MCM003",
    },
    {
        FIELD_COORDINATE: "A01",
        FIELD_SOURCE: "test1",
        FIELD_RESULT: "Positive",
        FIELD_PLATE_BARCODE: "456",
        "released": True,
        FIELD_ROOT_SAMPLE_ID: "MCM004",
    },
]


FILTERED_POSITIVE_TESTING_SAMPLES: List[Dict[str, Union[str, bool]]] = [
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
        FIELD_ROOT_SAMPLE_ID: "MCM003",
        FIELD_FILTERED_POSITIVE: False,
        FIELD_FILTERED_POSITIVE_TIMESTAMP: "2020-01-01T00:00:00.000Z",
        FIELD_FILTERED_POSITIVE_VERSION: "v0",
        FIELD_CREATED_AT: dateutil.parser.parse(FILTERED_POSITIVE_FIELDS_SET_DATE) - timedelta(days=1),
    },
    {
        FIELD_COORDINATE: "D01",
        FIELD_SOURCE: "test1",
        FIELD_RESULT: POSITIVE_RESULT_VALUE,
        FIELD_PLATE_BARCODE: "123",
        FIELD_ROOT_SAMPLE_ID: "MCM003",
        FIELD_CREATED_AT: dateutil.parser.parse(FILTERED_POSITIVE_FIELDS_SET_DATE) - timedelta(days=1),
    },
    {
        FIELD_COORDINATE: "D01",
        FIELD_SOURCE: "test2",
        FIELD_RESULT: POSITIVE_RESULT_VALUE,
        FIELD_PLATE_BARCODE: "456",
        FIELD_ROOT_SAMPLE_ID: "MCM004",
        FIELD_CREATED_AT: dateutil.parser.parse(FILTERED_POSITIVE_FIELDS_SET_DATE) - timedelta(days=2),
    },
]


UNMIGRATED_MONGO_TESTING_SAMPLES: List[Dict[str, Union[str, bool]]] = [
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

MIGRATED_MONGO_TESTING_SAMPLES = [
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


MLWH_SAMPLES = [
    {
        MLWH_MONGODB_ID: "1",
        MLWH_COORDINATE: "A1",
        MLWH_PLATE_BARCODE: "123",
        MLWH_ROOT_SAMPLE_ID: "MCM001",
        MLWH_RNA_ID: "AAA123",
        MLWH_RESULT: POSITIVE_RESULT_VALUE,
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
        MLWH_RESULT: POSITIVE_RESULT_VALUE,
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


@pytest.fixture
def samples_collection_accessor(mongo_database):
    return get_mongo_collection(mongo_database[1], COLLECTION_SAMPLES)


@pytest.fixture
def centres_collection_accessor(mongo_database):
    return get_mongo_collection(mongo_database[1], COLLECTION_CENTRES)


@pytest.fixture
def samples_history_collection_accessor(mongo_database):
    return get_mongo_collection(mongo_database[1], COLLECTION_SAMPLES_HISTORY)


@pytest.fixture
def testing_samples(samples_collection_accessor):
    result = samples_collection_accessor.insert_many(TESTING_SAMPLES)
    samples = list(samples_collection_accessor.find({"_id": {"$in": result.inserted_ids}}))
    try:
        yield samples
    finally:
        samples_collection_accessor.delete_many({})


@pytest.fixture
def filtered_positive_testing_samples(samples_collection_accessor):
    result = samples_collection_accessor.insert_many(FILTERED_POSITIVE_TESTING_SAMPLES)
    samples = list(samples_collection_accessor.find({"_id": {"$in": result.inserted_ids}}))
    try:
        yield samples
    finally:
        samples_collection_accessor.delete_many({})


@pytest.fixture
def filtered_positive_testing_samples_no_version_set(samples_collection_accessor):
    samples = copy.deepcopy(FILTERED_POSITIVE_TESTING_SAMPLES)
    del samples[3][FIELD_FILTERED_POSITIVE]

    result = samples_collection_accessor.insert_many(samples)
    samples = list(samples_collection_accessor.find({"_id": {"$in": result.inserted_ids}}))
    try:
        yield samples
    finally:
        samples_collection_accessor.delete_many({})


@pytest.fixture
def unmigrated_mongo_testing_samples(config, event_wh_sql_engine):
    return UNMIGRATED_MONGO_TESTING_SAMPLES


@pytest.fixture
def migrated_mongo_testing_samples(config, event_wh_sql_engine):
    return MIGRATED_MONGO_TESTING_SAMPLES


@pytest.fixture
def mlwh_samples(config, event_wh_sql_engine):
    return MLWH_SAMPLES


@pytest.fixture
def event_wh_data(config, event_wh_sql_engine):
    try:
        subjects_table = get_table(event_wh_sql_engine, config.EVENT_WH_SUBJECTS_TABLE)
        roles_table = get_table(event_wh_sql_engine, config.EVENT_WH_ROLES_TABLE)
        events_table = get_table(event_wh_sql_engine, config.EVENT_WH_EVENTS_TABLE)
        event_types_table = get_table(event_wh_sql_engine, config.EVENT_WH_EVENT_TYPES_TABLE)
        subject_types_table = get_table(event_wh_sql_engine, config.EVENT_WH_SUBJECT_TYPES_TABLE)
        role_types_table = get_table(event_wh_sql_engine, config.EVENT_WH_ROLE_TYPES_TABLE)

        def delete_event_warehouse_data():
            with event_wh_sql_engine.begin() as connection:
                connection.execute(roles_table.delete())
                connection.execute(subjects_table.delete())
                connection.execute(events_table.delete())
                connection.execute(event_types_table.delete())
                connection.execute(subject_types_table.delete())
                connection.execute(role_types_table.delete())

        delete_event_warehouse_data()
        with event_wh_sql_engine.begin() as connection:
            print("Inserting Events Warehouse test data")
            connection.execute(role_types_table.insert(), EVENT_WH_DATA["role_types"])
            connection.execute(event_types_table.insert(), EVENT_WH_DATA["event_types"])
            connection.execute(subject_types_table.insert(), EVENT_WH_DATA["subject_types"])
            connection.execute(subjects_table.insert(), EVENT_WH_DATA["subjects"])
            connection.execute(events_table.insert(), EVENT_WH_DATA["events"])
            connection.execute(roles_table.insert(), EVENT_WH_DATA["roles"])
        yield
    finally:
        delete_event_warehouse_data()


@pytest.fixture
def mlwh_sentinel_cherrypicked(config, mlwh_sql_engine):
    def delete_data():
        delete_from_mlwh(mlwh_sql_engine, config.MLWH_STOCK_RESOURCES_TABLE)
        delete_from_mlwh(mlwh_sql_engine, config.MLWH_SAMPLE_TABLE)
        delete_from_mlwh(mlwh_sql_engine, config.MLWH_STUDY_TABLE)

    try:
        delete_data()

        # inserts
        insert_into_mlwh(MLWH_SAMPLE_STOCK_RESOURCE["sample"], mlwh_sql_engine, config.MLWH_SAMPLE_TABLE)
        insert_into_mlwh(MLWH_SAMPLE_STOCK_RESOURCE["study"], mlwh_sql_engine, config.MLWH_STUDY_TABLE)
        insert_into_mlwh(
            MLWH_SAMPLE_STOCK_RESOURCE["stock_resource"],
            mlwh_sql_engine,
            config.MLWH_STOCK_RESOURCES_TABLE,
        )

        yield
    finally:
        delete_data()


@pytest.fixture
def mlwh_beckman_cherrypicked(config, mlwh_sql_engine):
    def delete_data():
        delete_from_mlwh(mlwh_sql_engine, config.MLWH_SAMPLE_TABLE)
        delete_from_mlwh(mlwh_sql_engine, config.MLWH_LIGHTHOUSE_SAMPLE_TABLE)

    try:
        delete_data()

        # inserts
        insert_into_mlwh(
            MLWH_SAMPLE_LIGHTHOUSE_SAMPLE["lighthouse_sample"],
            mlwh_sql_engine,
            config.MLWH_LIGHTHOUSE_SAMPLE_TABLE,
        )
        insert_into_mlwh(
            MLWH_SAMPLE_LIGHTHOUSE_SAMPLE["sample"],
            mlwh_sql_engine,
            config.MLWH_SAMPLE_TABLE,
        )

        yield
    finally:
        delete_data()


@pytest.fixture
def mlwh_sentinel_and_beckman_cherrypicked(config, mlwh_sql_engine):
    def delete_data():
        delete_from_mlwh(mlwh_sql_engine, config.MLWH_STOCK_RESOURCES_TABLE)
        delete_from_mlwh(mlwh_sql_engine, config.MLWH_SAMPLE_TABLE)
        delete_from_mlwh(mlwh_sql_engine, config.MLWH_STUDY_TABLE)
        delete_from_mlwh(mlwh_sql_engine, config.MLWH_LIGHTHOUSE_SAMPLE_TABLE)

    try:
        delete_data()

        # inserts
        insert_into_mlwh(
            MLWH_SAMPLE_LIGHTHOUSE_SAMPLE["lighthouse_sample"],
            mlwh_sql_engine,
            config.MLWH_LIGHTHOUSE_SAMPLE_TABLE,
        )
        insert_into_mlwh(
            MLWH_SAMPLE_STOCK_RESOURCE["sample"] + MLWH_SAMPLE_LIGHTHOUSE_SAMPLE["sample"],
            mlwh_sql_engine,
            config.MLWH_SAMPLE_TABLE,
        )
        insert_into_mlwh(
            MLWH_SAMPLE_STOCK_RESOURCE["study"],
            mlwh_sql_engine,
            config.MLWH_STUDY_TABLE,
        )
        insert_into_mlwh(
            MLWH_SAMPLE_STOCK_RESOURCE["stock_resource"],
            mlwh_sql_engine,
            config.MLWH_STOCK_RESOURCES_TABLE,
        )

        yield
    finally:
        delete_data()


def insert_into_mlwh(data, mlwh_sql_engine, table_name):
    table = get_table(mlwh_sql_engine, table_name)

    with mlwh_sql_engine.begin() as connection:
        connection.execute(table.delete())  # delete all rows from table first
        print("Inserting MLWH test data")
        connection.execute(table.insert(), data)


def delete_from_mlwh(mlwh_sql_engine, table_name):
    table = get_table(mlwh_sql_engine, table_name)

    with mlwh_sql_engine.begin() as connection:
        print("Deleting MLWH test data")
        connection.execute(table.delete())


def get_table(sql_engine: Engine, table_name: str):
    metadata = MetaData(sql_engine)
    metadata.reflect()
    return metadata.tables[table_name]


@pytest.fixture
def event_wh_sql_engine(config):
    create_engine_string = f"mysql+pymysql://{config.WAREHOUSES_RW_CONN_STRING}/{config.EVENTS_WH_DB}"
    return sqlalchemy.create_engine(create_engine_string, pool_recycle=3600)


@pytest.fixture
def mlwh_sql_engine(config):
    create_engine_string = f"mysql+pymysql://{config.WAREHOUSES_RW_CONN_STRING}/{config.ML_WH_DB}"
    return sqlalchemy.create_engine(create_engine_string, pool_recycle=3600)


@pytest.fixture
def testing_centres(centres_collection_accessor, config):
    result = centres_collection_accessor.insert_many(config.CENTRES)
    try:
        yield result
    finally:
        centres_collection_accessor.delete_many({})


@pytest.fixture
def cleanup_backups():
    """Fixture to remove the tmp/backups directory when complete."""
    try:
        yield
    finally:
        shutil.rmtree("tmp/backups")


@pytest.fixture
def blacklist_for_centre(config):
    try:
        config.CENTRES[0]["file_names_to_ignore"] = ["AP_sanger_report_200503_2338.csv"]
        yield config
    finally:
        config.CENTRES[0]["file_names_to_ignore"] = []
