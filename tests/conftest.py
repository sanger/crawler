import copy
import logging
import logging.config
import shutil
from unittest.mock import patch
import pytest
import sqlalchemy
from sqlalchemy import MetaData

from crawler.constants import (
    COLLECTION_CENTRES,
    COLLECTION_SAMPLES,
    COLLECTION_PRIORITY_SAMPLES,
    COLLECTION_SAMPLES_HISTORY,
    FIELD_FILTERED_POSITIVE,
    FIELD_MONGODB_ID,
    MLWH_TABLE_NAME,
)
from crawler.db.mongo import create_mongo_client, get_mongo_collection, get_mongo_db
from crawler.db.mysql import create_mysql_connection
from crawler.file_processing import Centre, CentreFile
from crawler.helpers.general_helpers import get_config
from tests.data.testing_objects import (
    EVENT_WH_DATA,
    FILTERED_POSITIVE_TESTING_SAMPLES,
    MLWH_SAMPLE_LIGHTHOUSE_SAMPLE,
    MLWH_SAMPLE_STOCK_RESOURCE,
    MLWH_SAMPLES_WITH_FILTERED_POSITIVE_FIELDS,
    MONGO_SAMPLES_WITH_FILTERED_POSITIVE_FIELDS,
    MONGO_SAMPLES_WITHOUT_FILTERED_POSITIVE_FIELDS,
    TESTING_SAMPLES,
    TESTING_PRIORITY_SAMPLES,
)

logger = logging.getLogger(__name__)
CONFIG, _ = get_config("crawler.config.test")
logging.config.dictConfig(CONFIG.LOGGING)


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


@pytest.fixture
def samples_collection_accessor(mongo_database):
    return get_mongo_collection(mongo_database[1], COLLECTION_SAMPLES)


@pytest.fixture
def priority_samples_collection_accessor(mongo_database):
    return get_mongo_collection(mongo_database[1], COLLECTION_PRIORITY_SAMPLES)


@pytest.fixture
def centres_collection_accessor(mongo_database):
    return get_mongo_collection(mongo_database[1], COLLECTION_CENTRES)


@pytest.fixture
def samples_history_collection_accessor(mongo_database):
    return get_mongo_collection(mongo_database[1], COLLECTION_SAMPLES_HISTORY)


@pytest.fixture
def testing_samples(samples_collection_accessor):
    result = samples_collection_accessor.insert_many(TESTING_SAMPLES)
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
