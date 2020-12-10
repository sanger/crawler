import logging
import logging.config
import shutil
from typing import Dict, List, Union, Any
from unittest.mock import patch
import sqlalchemy  # type: ignore
from sqlalchemy.engine.base import Engine  # type: ignore
from sqlalchemy import MetaData  # type: ignore

import pytest
from crawler.constants import (
    COLLECTION_CENTRES,
    COLLECTION_SAMPLES,
    COLLECTION_SAMPLES_HISTORY,
    FIELD_COORDINATE,
    FIELD_PLATE_BARCODE,
    FIELD_RESULT,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_SOURCE,
    MLWH_TABLE_NAME,
    FIELD_FILTERED_POSITIVE,
    FIELD_FILTERED_POSITIVE_TIMESTAMP,
    FIELD_FILTERED_POSITIVE_VERSION,
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
    # Copy the test files to a new directory, as we expect run
    # to perform a clean up, and we don't want it cleaning up our
    # main copy of the data. We don't disable the clean up as:
    # 1) It also clears up the master files, which we'd otherwise need to handle
    # TODO: remove reference to master files above - they don't exist anymore
    # 2) It means we keep the tested process closer to the actual one
    _ = shutil.copytree("tests/files", "tmp/files", dirs_exist_ok=True)
    try:
        yield
    finally:
        # remove files https://docs.python.org/3/library/shutil.html#shutil.rmtree
        shutil.rmtree("tmp/files")
        # (_, _, files) = next(os.walk("tmp/files"))


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
        FIELD_FILTERED_POSITIVE_VERSION: "v1",
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
        FIELD_FILTERED_POSITIVE_VERSION: "v0",
    },
    {
        FIELD_COORDINATE: "C01",
        FIELD_SOURCE: "test1",
        FIELD_RESULT: "Void",
        FIELD_PLATE_BARCODE: "123",
        FIELD_ROOT_SAMPLE_ID: "MCM003",
        FIELD_FILTERED_POSITIVE: False,
        FIELD_FILTERED_POSITIVE_TIMESTAMP: "2020-01-01T00:00:00.000Z",
        FIELD_FILTERED_POSITIVE_VERSION: "v0",
    },
    {
        FIELD_COORDINATE: "D01",
        FIELD_SOURCE: "test1",
        FIELD_RESULT: "Void",
        FIELD_PLATE_BARCODE: "123",
        FIELD_ROOT_SAMPLE_ID: "MCM003",
    },
]


EVENT_WH_DATA: Dict[str, Any] = {
    "subjects": [
        {"id": 1, "uuid": "1".encode("utf-8"), "friendly_name": "ss1", "subject_type_id": 1},
        {"id": 2, "uuid": "2".encode("utf-8"), "friendly_name": "ss2", "subject_type_id": 1},
    ],
    "roles": [
        {"id": 1, "event_id": 1, "subject_id": 1, "role_type_id": 1},
        {"id": 2, "event_id": 2, "subject_id": 2, "role_type_id": 1},
    ],
    "events": [
        {
            "id": 1,
            "lims_id": "SQSCP",
            "uuid": "1".encode("utf-8"),
            "event_type_id": 1,
            "occured_at": "2020-09-25 11:35:30", #
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
        }
    ],
    "event_types": [{"id": 1, "key": "cherrypick_layout_set", "description": "stuff"}],
    "subject_types": [{"id": 1, "key": "sample", "description": "stuff"}],
    "role_types": [{"id": 1, "key": "sample", "description": "stuff"}],
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
def v1_filtered_positive_testing_samples(samples_collection_accessor):
    V1_FILTERED_POSITIVE_TESTING_SAMPLES = FILTERED_POSITIVE_TESTING_SAMPLES
    del V1_FILTERED_POSITIVE_TESTING_SAMPLES[1:3]

    result = samples_collection_accessor.insert_many(V1_FILTERED_POSITIVE_TESTING_SAMPLES)
    samples = list(samples_collection_accessor.find({"_id": {"$in": result.inserted_ids}}))
    try:
        yield samples
    finally:
        samples_collection_accessor.delete_many({})


@pytest.fixture
def event_wh_data(config):
    insert_data_into_events_warehouse_tables(config, EVENT_WH_DATA, event_wh_sql_engine(config))


def insert_data_into_events_warehouse_tables(config, data, event_wh_sql_engine):
    subjects_table = get_table(event_wh_sql_engine, config.EVENT_WH_SUBJECTS_TABLE)
    roles_table = get_table(event_wh_sql_engine, config.EVENT_WH_ROLES_TABLE)
    events_table = get_table(event_wh_sql_engine, config.EVENT_WH_EVENTS_TABLE)
    event_types_table = get_table(event_wh_sql_engine, config.EVENT_WH_EVENT_TYPES_TABLE)
    subject_types_table = get_table(event_wh_sql_engine, config.EVENT_WH_SUBJECT_TYPES_TABLE)
    role_types_table = get_table(event_wh_sql_engine, config.EVENT_WH_ROLE_TYPES_TABLE)

    with event_wh_sql_engine.begin() as connection:
        # delete all rows from each table
        connection.execute(roles_table.delete())
        connection.execute(subjects_table.delete())
        connection.execute(events_table.delete())
        connection.execute(event_types_table.delete())
        connection.execute(subject_types_table.delete())
        connection.execute(role_types_table.delete())

        print("Inserting Events Warehouse test data")
        connection.execute(role_types_table.insert(), data["role_types"])
        connection.execute(event_types_table.insert(), data["event_types"])
        connection.execute(subject_types_table.insert(), data["subject_types"])
        connection.execute(subjects_table.insert(), data["subjects"])
        connection.execute(events_table.insert(), data["events"])
        connection.execute(roles_table.insert(), data["roles"])


def get_table(sql_engine: Engine, table_name: str):
    metadata = MetaData(sql_engine)
    metadata.reflect()
    return metadata.tables[table_name]


def event_wh_sql_engine(config):
    create_engine_string = f"mysql+pymysql://{config.WAREHOUSES_RW_CONN_STRING}/{config.EVENTS_WH_DB}"
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
