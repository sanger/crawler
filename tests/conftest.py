import logging
import logging.config
import shutil
from typing import Dict, List, Union, Any
from unittest.mock import patch
import sqlalchemy  # type: ignore
from sqlalchemy.engine.base import Engine  # type: ignore
from sqlalchemy import MetaData  # type: ignore
from datetime import datetime, timedelta

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
    V0_V1_CUTOFF_DATE,
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
        {"id": 3, "uuid": "3".encode("utf-8"), "friendly_name": "ss3", "subject_type_id": 1},
    ],
    "roles": [
        {"id": 1, "event_id": 1, "subject_id": 1, "role_type_id": 1},
        {"id": 2, "event_id": 2, "subject_id": 2, "role_type_id": 1},
        {"id": 3, "event_id": 3, "subject_id": 3, "role_type_id": 1},
    ],
    "events": [
        {
            "id": 1,
            "lims_id": "SQSCP",
            "uuid": "1".encode("utf-8"),
            "event_type_id": 1,
            "occured_at": "2020-09-25 11:35:30",  #
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
    ],
    "event_types": [{"id": 1, "key": "cherrypick_layout_set", "description": "stuff"}],
    "subject_types": [{"id": 1, "key": "sample", "description": "stuff"}],
    "role_types": [{"id": 1, "key": "sample", "description": "stuff"}],
}


def to_datetime(date_string):
    return datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")


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
            "created": str(to_datetime(V0_V1_CUTOFF_DATE)),  # Created at cut-off time
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
            "created": str(to_datetime(V0_V1_CUTOFF_DATE) - timedelta(days=1)),  # Created before cut-off time
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
            "created": str(to_datetime(V0_V1_CUTOFF_DATE) + timedelta(days=1)),  # Created before cut-off time,
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
def mlwh_sample_stock_resource(config, mlwh_sql_engine):
    # deletes
    delete_from_mlwh(
        MLWH_SAMPLE_STOCK_RESOURCE["stock_resource"],
        mlwh_sql_engine,
        config.MLWH_STOCK_RESOURCES_TABLE,
    )
    delete_from_mlwh(MLWH_SAMPLE_STOCK_RESOURCE["sample"], mlwh_sql_engine, config.MLWH_SAMPLE_TABLE)
    delete_from_mlwh(MLWH_SAMPLE_STOCK_RESOURCE["study"], mlwh_sql_engine, config.MLWH_STUDY_TABLE)

    # inserts
    insert_into_mlwh(MLWH_SAMPLE_STOCK_RESOURCE["sample"], mlwh_sql_engine, config.MLWH_SAMPLE_TABLE)
    insert_into_mlwh(MLWH_SAMPLE_STOCK_RESOURCE["study"], mlwh_sql_engine, config.MLWH_STUDY_TABLE)
    insert_into_mlwh(
        MLWH_SAMPLE_STOCK_RESOURCE["stock_resource"],
        mlwh_sql_engine,
        config.MLWH_STOCK_RESOURCES_TABLE,
    )


def insert_into_mlwh(data, mlwh_sql_engine, table_name):
    table = get_table(mlwh_sql_engine, table_name)

    with mlwh_sql_engine.begin() as connection:
        connection.execute(table.delete())  # delete all rows from table first
        print("Inserting MLWH test data")
        connection.execute(table.insert(), data)


def delete_from_mlwh(data, mlwh_sql_engine, table_name):
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
    sql_engine = sqlalchemy.create_engine(
        (
            f"mysql+pymysql://{config.MLWH_DB_RO_USER}:{config.MLWH_DB_RO_PASSWORD}"  # type: ignore
            f"@{config.MLWH_DB_HOST}/{config.EVENTS_WH_DB}"  # type: ignore
        ),
        pool_recycle=3600,
    )
    return sql_engine


@pytest.fixture
def mlwh_sql_engine(config):
    sql_engine = sqlalchemy.create_engine(
        (
            f"mysql+pymysql://{config.MLWH_DB_RO_USER}:{config.MLWH_DB_RO_PASSWORD}"  # type: ignore
            f"@{config.MLWH_DB_HOST}/{config.ML_WH_DB}"  # type: ignore
        ),
        pool_recycle=3600,
    )
    return sql_engine


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
