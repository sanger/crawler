import logging
from datetime import datetime
from types import ModuleType
from typing import Dict, List, Iterator
from crawler.helpers import current_time

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError, OperationFailure
from pymongo.results import InsertOneResult

from contextlib import contextmanager

import mysql.connector as mysql  # type: ignore
from mysql.connector.connection_cext import CMySQLConnection  # type: ignore
from mysql.connector import Error  # type: ignore
from crawler.sql_queries import SQL_MLWH_MULTIPLE_INSERT, SQL_TEST_MLWH_CREATE
from crawler.helpers import get_config

import pyodbc  # type: ignore
from crawler.constants import (
    DART_STATE_PENDING,
    DART_STATE_PROPERTY_NAME,
    DART_GET_PLATE_PROPERTY_SQL,
    DART_SET_PLATE_PROPERTY_SQL,
    DART_SET_PROP_STATUS_SUCCESS,
)

logger = logging.getLogger(__name__)


def create_mongo_client(config: ModuleType) -> MongoClient:
    """Create a MongoClient with the given config parameters.

    Arguments:
        config {ModuleType} -- application config specifying host and port

    Returns:
        MongoClient -- a client used to interact with the database server
    """
    try:
        logger.debug(f"Connecting to mongo")
        mongo_uri = config.MONGO_URI  # type: ignore
        return MongoClient(mongo_uri)
    except AttributeError as e:
        #  there is no MONGO_URI so try each config separately
        # logger.warning(e)

        mongo_host = config.MONGO_HOST  # type: ignore
        mongo_port = config.MONGO_PORT  # type: ignore
        mongo_username = config.MONGO_USERNAME  # type: ignore
        mongo_password = config.MONGO_PASSWORD  # type: ignore
        mongo_db = config.MONGO_DB  # type: ignore

        logger.debug(f"Connecting to {mongo_host} on port {mongo_port}")

        return MongoClient(
            host=mongo_host,
            port=mongo_port,
            username=mongo_username,
            password=mongo_password,
            authSource=mongo_db,
        )


def get_mongo_db(config: ModuleType, client: MongoClient) -> Database:
    """Get a handle on a mongodb database - remember that it is lazy and is only created when
    documents are added to a collection.

    Arguments:
        config {ModuleType} -- application config specifying the database
        client {MongoClient} -- the client to use for the connection

    Returns:
        Database -- a reference to the database in mongo
    """
    db = config.MONGO_DB  # type: ignore

    logger.debug(f"Get database '{db}'")

    return client[db]


def get_mongo_collection(database: Database, collection_name: str) -> Collection:
    """Get a reference to a mongo collection from a database. A collection is created when documents
    are written to it.

    Arguments:
        database {Database} -- the database to get a collection from
        collection_name {str} -- the name of the collection to get/create

    Returns:
        Collection -- a reference to the collection
    """
    logger.debug(f"Get collection '{collection_name}'")

    return database[collection_name]


@contextmanager
def samples_collection_accessor(database: Database, collection_name: str) -> Iterator[Collection]:
    logger.debug(f"Opening collection: {collection_name}")
    temporary_collection = get_mongo_collection(database, collection_name)

    yield temporary_collection


def create_import_record(
    import_collection: Collection,
    centre: Dict[str, str],
    docs_inserted: int,
    file_name: str,
    errors: List[str],
) -> InsertOneResult:
    """Creates and inserts an import record for a centre.

    Arguments:
        import_collection {Collection} -- the collection which stores import status documents
        centre {Dict[str, str]} -- the centre for which to store the import status
        docs_inserted {int} -- to number of documents inserted for this centre
        file_name {str} -- file parsed for samples
        errors {List[str]} -- a list of errors while trying to process this centre

    Returns:
        InsertOneResult -- the result of inserting this document
    """
    logger.debug(f"Creating the status record for {centre['name']}")

    status_doc = {
        "date": datetime.now().isoformat(timespec="seconds"),
        "centre_name": centre["name"],
        "csv_file_used": file_name,
        "number_of_records": docs_inserted,
        "errors": errors,
    }

    return import_collection.insert_one(status_doc)


def populate_centres_collection(
    collection: Collection, documents: List[Dict[str, str]], filter_field: str
) -> None:
    """Populates a collection using the given documents. It uses the filter_field to replace any
    documents that match the filter and adds any new documents.

    Arguments:
        collection {Collection} -- collection to populate
        documents {List[Dict[str, str]]} -- documents to populate the collection with
        filter_field {str} -- filter to search for matching documents
    """
    logger.debug(
        f"Populating/updating '{collection.full_name}' using '{filter_field}' as the filter"
    )

    for document in documents:
        _ = collection.find_one_and_update(
            {filter_field: document[filter_field]}, {"$set": document}, upsert=True
        )


def create_mysql_connection(config: ModuleType, readonly=True) -> CMySQLConnection:
    """Create a CMySQLConnection with the given config parameters.

    Arguments:
        config {ModuleType} -- application config specifying database details

    Returns:
        CMySQLConnection -- a client used to interact with the database server
    """
    mlwh_db_host = config.MLWH_DB_HOST  # type: ignore
    mlwh_db_port = config.MLWH_DB_PORT  # type: ignore
    if readonly:
        mlwh_db_username = config.MLWH_DB_RO_USER  # type: ignore
        mlwh_db_password = config.MLWH_DB_RO_PASSWORD  # type: ignore
    else:
        mlwh_db_username = config.MLWH_DB_RW_USER  # type: ignore
        mlwh_db_password = config.MLWH_DB_RW_PASSWORD  # type: ignore
    mlwh_db_db = config.MLWH_DB_DBNAME  # type: ignore

    logger.debug(f"Attempting to connect to {mlwh_db_host} on port {mlwh_db_port}")

    mysql_conn = None
    try:
        mysql_conn = mysql.connect(
            host=mlwh_db_host,
            port=mlwh_db_port,
            username=mlwh_db_username,
            password=mlwh_db_password,
            database=mlwh_db_db,
            # whether to use pure python or the C extension.
            # default is false, but specify it so more predictable
            use_pure=False,
        )
        if mysql_conn is not None:
            if mysql_conn.is_connected():
                logger.debug("MySQL Connection Successful")
            else:
                logger.error("MySQL Connection Failed")

    except Error as e:
        logger.error(f"Exception on connecting to MySQL database: {e}")

    return mysql_conn


def run_mysql_executemany_query(
    mysql_conn: CMySQLConnection, sql_query: str, values: List[Dict[str, str]]
) -> None:
    """Writes the sample testing information into the MLWH.

    Arguments:
        mysql_conn {CMySQLConnection} -- a client used to interact with the database server
        sql_query {str} -- the SQL query to run (see sql_queries.py)
        values {List[Dict[str, str]]} -- array of value hashes representing documents inserted into the Mongo DB
    """
    ## fetch the cursor from the DB connection
    cursor = mysql_conn.cursor()

    try:
        ## executing the query with values
        num_values = len(values)

        # BN. If ROWS_PER_QUERY value is too high, you may get '2006 (HY000): MySQL server has gone away' error
        # indicating you've exceeded the max_allowed_packet size for MySQL
        ROWS_PER_QUERY = 25000
        values_index = 0
        total_rows_affected = 0
        logger.debug(
            f"Attempting to insert or update {num_values} rows in the MLWH database in batches of {ROWS_PER_QUERY}"
        )

        while values_index < num_values:
            logger.debug(
                f"Inserting records between {values_index} and {values_index + ROWS_PER_QUERY}"
            )
            cursor.executemany(sql_query, values[values_index : values_index + ROWS_PER_QUERY])
            logger.debug(
                f"{cursor.rowcount} rows affected in MLWH. (Note: each updated row increases the count by 2, instead of 1)"
            )
            total_rows_affected += cursor.rowcount
            values_index += ROWS_PER_QUERY
            logger.debug("Committing changes to MLWH database.")
            mysql_conn.commit()

        # number of rows affected using cursor.rowcount - not easy to interpret:
        # reports 1 per inserted row,
        # 2 per updated existing row,
        # and 0 per unchanged existing row
        logger.debug(
            f"A total of {total_rows_affected} rows were affected in MLWH. (Note: each updated row increases the count by 2, instead of 1)"
        )
    except:
        logger.error("MLWH database executemany transaction failed")
        raise
    finally:
        # close the cursor
        logger.debug("Closing the cursor.")
        cursor.close()

        # close the connection
        logger.debug("Closing the MLWH database connection.")
        mysql_conn.close()


# Set up a basic MLWH db for testing
def init_warehouse_db_command():
    """Drop and recreate required tables."""
    logger.debug("Initialising the test MySQL warehouse database")
    config, _settings_module = get_config("crawler.config.development")
    mysql_conn = create_mysql_connection(config, False)
    mysql_cursor = mysql_conn.cursor()

    for result in mysql_cursor.execute(SQL_TEST_MLWH_CREATE, multi=True):
        if result.with_rows:
            result.fetchall()

    mysql_conn.commit()
    mysql_cursor.close()
    mysql_conn.close()

    logger.debug("Done")


def create_dart_sql_server_conn(config: ModuleType, readonly=True) -> pyodbc.Connection:
    """Create a SQL Server connection to DART with the given config parameters.

    Arguments:
        config {ModuleType} -- application config specifying database details

    Returns:
        pyodbc.Connection -- connection object used to interact with the sql server database
    """
    dart_db_host = config.DART_DB_HOST  # type: ignore
    dart_db_port = config.DART_DB_PORT  # type: ignore
    if readonly:
        dart_db_username = config.DART_DB_RO_USER  # type: ignore
        dart_db_password = config.DART_DB_RO_PASSWORD  # type: ignore
    else:
        dart_db_username = config.DART_DB_RW_USER  # type: ignore
        dart_db_password = config.DART_DB_RW_PASSWORD  # type: ignore
    dart_db_db = config.DART_DB_DBNAME  # type: ignore
    dart_db_driver = config.DART_DB_DRIVER  # type: ignore

    connection_string = (
        "DRIVER="
        + dart_db_driver
        + ";SERVER="
        + dart_db_host
        + f";PORT={dart_db_port};DATABASE="
        + dart_db_db
        + ";UID="
        + dart_db_username
        + ";PWD="
        + dart_db_password
    )

    logger.debug(f"Attempting to connect to {dart_db_host} on port {dart_db_port}")

    sql_server_conn = None
    try:
        sql_server_conn = pyodbc.connect(connection_string)

        if sql_server_conn is not None:
            logger.debug("DART Connection Successful")
        else:
            logger.error("DART Connection Failed")

    except Error as e:
        logger.error(f"Exception on connecting to DART database: {e}")

    return sql_server_conn


def get_dart_plate_state(cursor: pyodbc.Cursor, plate_barcode: str) -> str:
    """Gets the state of a DART plate.

    Arguments:
        cursor {pyodbc.Cursor} -- The cursor with with to execute queries.
        plate_barcode {str} -- The barcode of the plate whose state to fetch.

    Returns:
        str -- The state of the plate in DART.
    """
    params = (plate_barcode, DART_STATE_PROPERTY_NAME)
    cursor.execute(DART_GET_PLATE_PROPERTY_SQL, params)
    return cursor.fetchval()


def set_dart_plate_state_pending(cursor: pyodbc.Cursor, plate_barcode: str) -> str:
    """Sets the state of a DART plate to pending.

    Arguments:
        cursor {pyodbc.Cursor} -- The cursor with with to execute queries.
        plate_barcode {str} -- The barcode of the plate whose state to set.

    Returns:
        bool -- Return True if DART was updated successfully, else False.
    """
    params = (plate_barcode, DART_STATE_PROPERTY_NAME, DART_STATE_PENDING)
    cursor.execute(DART_SET_PLATE_PROPERTY_SQL, params)
    response = cursor.fetchval()
    return response == DART_SET_PROP_STATUS_SUCCESS
