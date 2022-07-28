import copy
import logging
import logging.config
import shutil
from datetime import datetime
from unittest.mock import patch

import pytest
import sqlalchemy
from sqlalchemy import MetaData

from crawler import create_app
from crawler.constants import (
    CENTRE_KEY_DATA_SOURCE,
    CENTRE_KEY_FILE_NAMES_TO_IGNORE,
    COLLECTION_CENTRES,
    COLLECTION_CHERRYPICK_TEST_DATA,
    COLLECTION_IMPORTS,
    COLLECTION_PRIORITY_SAMPLES,
    COLLECTION_SAMPLES,
    COLLECTION_SOURCE_PLATES,
    FIELD_FILTERED_POSITIVE,
    FIELD_MONGODB_ID,
    MLWH_TABLE_NAME,
)
from crawler.db.mongo import create_mongo_client, get_mongo_collection, get_mongo_db
from crawler.db.mysql import create_mysql_connection
from crawler.file_processing import Centre, CentreFile
from crawler.helpers.db_helpers import ensure_mongo_collections_indexed
from crawler.helpers.general_helpers import get_config, get_sftp_connection
from tests.testing_objects import (
    EVENT_WH_DATA,
    FILTERED_POSITIVE_TESTING_SAMPLES,
    MLWH_SAMPLE_LIGHTHOUSE_SAMPLE,
    MLWH_SAMPLE_STOCK_RESOURCE,
    MLWH_SAMPLE_UNCONNECTED_LIGHTHOUSE_SAMPLE,
    MLWH_SAMPLE_WITH_LAB_ID_LIGHTHOUSE_SAMPLE,
    MLWH_SAMPLES_WITH_FILTERED_POSITIVE_FIELDS,
    MONGO_SAMPLES_WITH_FILTERED_POSITIVE_FIELDS,
    MONGO_SAMPLES_WITHOUT_FILTERED_POSITIVE_FIELDS,
    TESTING_PRIORITY_SAMPLES,
    TESTING_SAMPLES,
    TESTING_SAMPLES_WITH_LAB_ID,
    TESTING_SOURCE_PLATES,
)

logger = logging.getLogger(__name__)
CONFIG, _ = get_config("crawler.config.test")
logging.config.dictConfig(CONFIG.LOGGING)


@pytest.fixture
def app():
    app = create_app("crawler.config.test")
    yield app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def config():
    return CONFIG


@pytest.fixture
def centre(config):
    yield Centre(config, config.CENTRES[0])


@pytest.fixture
def centre_file(centre):
    yield CentreFile("some_file.csv", centre)


@pytest.fixture
def mongo_client(config):
    with create_mongo_client(config) as client:
        yield config, client


@pytest.fixture
def mongo_collections():
    return [
        COLLECTION_IMPORTS,
        COLLECTION_SAMPLES,
        COLLECTION_PRIORITY_SAMPLES,
        COLLECTION_SOURCE_PLATES,
        COLLECTION_CHERRYPICK_TEST_DATA,
    ]


@pytest.fixture
def mongo_database(mongo_client):
    config, mongo_client = mongo_client
    db = get_mongo_db(config, mongo_client)

    # Ensure any existing data is gone before a test starts
    mongo_client.drop_database(db)

    # Create indexes on collections -- this also creates the empty source_plates and samples collections
    ensure_mongo_collections_indexed(db)

    yield config, db


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
def mlwh_rw_db(mlwh_connection):
    try:
        cursor = mlwh_connection.cursor()
        yield (mlwh_connection, cursor)
    finally:
        cursor.close()


@pytest.fixture
def pyodbc_conn():
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
    _ = shutil.copytree("tests/test_files/good", "tmp/files", dirs_exist_ok=True)
    try:
        yield
    finally:
        # remove files https://docs.python.org/3/library/shutil.html#shutil.rmtree
        shutil.rmtree("tmp/files")


def samples_collection_with_samples(mongo_database, samples=None):
    samples_collection = get_mongo_collection(mongo_database, COLLECTION_SAMPLES)
    samples_collection.delete_many({})

    if samples and len(samples) > 0:
        samples_collection.insert_many(samples)

    return samples_collection


@pytest.fixture(params=[[]])
def samples_collection_accessor(mongo_database, request):
    return samples_collection_with_samples(mongo_database[1], request.param)


@pytest.fixture
def source_plates_collection_accessor(mongo_database):
    return get_mongo_collection(mongo_database[1], COLLECTION_SOURCE_PLATES)


@pytest.fixture
def imports_collection_accessor(mongo_database):
    return get_mongo_collection(mongo_database[1], COLLECTION_IMPORTS)


@pytest.fixture
def priority_samples_collection_accessor(mongo_database):
    return get_mongo_collection(mongo_database[1], COLLECTION_PRIORITY_SAMPLES)


@pytest.fixture
def centres_collection_accessor(mongo_database):
    return get_mongo_collection(mongo_database[1], COLLECTION_CENTRES)


@pytest.fixture
def testing_samples(mongo_database):
    samples_collection = samples_collection_with_samples(mongo_database[1], TESTING_SAMPLES)
    return list(samples_collection.find({}))


@pytest.fixture
def testing_source_plates(source_plates_collection_accessor):
    result = source_plates_collection_accessor.insert_many(TESTING_SOURCE_PLATES)
    # source_plates = list(source_plates_collection_accessor.find({FIELD_MONGODB_ID: {"$in": result.inserted_ids}}))
    try:
        # yield source_plates
        yield result
    finally:
        source_plates_collection_accessor.delete_many({})


@pytest.fixture
def testing_samples_with_lab_id(samples_collection_accessor):
    result = samples_collection_accessor.insert_many(TESTING_SAMPLES_WITH_LAB_ID)
    samples = list(samples_collection_accessor.find({FIELD_MONGODB_ID: {"$in": result.inserted_ids}}))
    try:
        yield samples
    finally:
        samples_collection_accessor.delete_many({})


@pytest.fixture
def testing_priority_samples(priority_samples_collection_accessor):
    result = priority_samples_collection_accessor.insert_many(TESTING_PRIORITY_SAMPLES)
    samples = list(priority_samples_collection_accessor.find({FIELD_MONGODB_ID: {"$in": result.inserted_ids}}))
    try:
        yield samples
    finally:
        priority_samples_collection_accessor.delete_many({})


@pytest.fixture
def filtered_positive_testing_samples(samples_collection_accessor):
    result = samples_collection_accessor.insert_many(FILTERED_POSITIVE_TESTING_SAMPLES)
    samples = list(samples_collection_accessor.find({FIELD_MONGODB_ID: {"$in": result.inserted_ids}}))
    try:
        yield samples
    finally:
        samples_collection_accessor.delete_many({})


@pytest.fixture
def filtered_positive_testing_samples_no_version_set(samples_collection_accessor):
    samples = copy.deepcopy(FILTERED_POSITIVE_TESTING_SAMPLES)
    del samples[3][FIELD_FILTERED_POSITIVE]

    result = samples_collection_accessor.insert_many(samples)
    samples = list(samples_collection_accessor.find({FIELD_MONGODB_ID: {"$in": result.inserted_ids}}))
    try:
        yield samples
    finally:
        samples_collection_accessor.delete_many({})


@pytest.fixture
def mongo_samples_without_filtered_positive_fields():
    return MONGO_SAMPLES_WITHOUT_FILTERED_POSITIVE_FIELDS


@pytest.fixture
def mongo_samples_with_filtered_positive_fields():
    return MONGO_SAMPLES_WITH_FILTERED_POSITIVE_FIELDS


@pytest.fixture
def mlwh_samples_with_filtered_positive_fields():
    return MLWH_SAMPLES_WITH_FILTERED_POSITIVE_FIELDS


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
def mlwh_samples_with_lab_id_for_migration(config, mlwh_sql_engine):
    def delete_data():
        delete_from_mlwh(mlwh_sql_engine, config.MLWH_LIGHTHOUSE_SAMPLE_TABLE)

    try:
        delete_data()

        # inserts
        insert_into_mlwh(
            MLWH_SAMPLE_WITH_LAB_ID_LIGHTHOUSE_SAMPLE["lighthouse_sample"],
            mlwh_sql_engine,
            config.MLWH_LIGHTHOUSE_SAMPLE_TABLE,
        )

        yield
    finally:
        delete_data()


@pytest.fixture
def mlwh_testing_samples_unconnected(config, mlwh_sql_engine):
    def delete_data():
        delete_from_mlwh(mlwh_sql_engine, config.MLWH_LIGHTHOUSE_SAMPLE_TABLE)

    try:
        delete_data()

        # inserts
        insert_into_mlwh(
            MLWH_SAMPLE_UNCONNECTED_LIGHTHOUSE_SAMPLE["lighthouse_sample"],
            mlwh_sql_engine,
            config.MLWH_LIGHTHOUSE_SAMPLE_TABLE,
        )

        yield
    finally:
        delete_data()


@pytest.fixture
def mlwh_cherrypicked_samples(config, mlwh_sql_engine):
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


def get_table(sql_engine, table_name):
    metadata = MetaData(sql_engine)
    metadata.reflect()
    return metadata.tables[table_name]


@pytest.fixture
def query_lighthouse_sample(mlwh_sql_engine):
    with mlwh_sql_engine.begin() as connection:
        yield connection


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
def test_data_source_centres(centres_collection_accessor, config):
    data_source_centres = [
        {
            CENTRE_KEY_DATA_SOURCE: "SFTP",
        },
        {
            CENTRE_KEY_DATA_SOURCE: "RabbitMQ",
        },
    ]

    centres_collection_accessor.insert_many(data_source_centres)
    try:
        yield centres_collection_accessor, config
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
        config.CENTRES[0][CENTRE_KEY_FILE_NAMES_TO_IGNORE] = ["AP_sanger_report_200503_2338.csv"]
        yield config
    finally:
        config.CENTRES[0][CENTRE_KEY_FILE_NAMES_TO_IGNORE] = []


def generate_new_object_for_string(original_str):
    """
    For checking bug on comparing strings with 'is'
    (Pdb) sample.get('Result', False) is 'Positive'
    <stdin>:1: SyntaxWarning: "is" with a literal. Did you mean "=="?
    False
    (Pdb) sample2.get('Result', False) is 'Positive'
    <stdin>:1: SyntaxWarning: "is" with a literal. Did you mean "=="?
    True
    """
    part1 = original_str[0:2]
    part2 = original_str[2:]
    new_str = part1 + part2
    return new_str


@pytest.fixture
def logging_messages():
    return {
        "success": {
            "msg": "Success",
        },
        "insert_failure": {
            "error_type": "TYPE 14",
            "msg": "Insert Failure",
            "critical_msg": "Insert Critical",
        },
        "connection_failure": {
            "error_type": "TYPE 15",
            "msg": "Connection Failure",
            "critical_msg": "Connection Critical",
        },
    }


@pytest.fixture
def downloadable_files(config):
    filenames = [
        "sftp/AP_sanger_report_200423_2214.csv",
        "sftp/AP_sanger_report_200423_2215.csv",
        "sftp/AP_sanger_report_200423_2216.csv",
        "sftp/AP_sanger_report_200423_2217.csv",
        "sftp/AP_sanger_report_200423_2218.csv",
    ]

    # Reset to current time
    with get_sftp_connection(config) as sftp:
        for filename in filenames:
            sftp.sftp_client.utime(filename, (datetime.now().timestamp(), datetime.now().timestamp()))

    return filenames
