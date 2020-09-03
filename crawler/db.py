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

import mysql.connector as mysql
from mysql.connector import MySQLConnection
from mysql.connector import Error

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

def create_mysql_connection(config: ModuleType, readonly = True) -> MySQLConnection:
    """Create a MySQLConnection with the given config parameters.

    Arguments:
        config {ModuleType} -- application config specifying database details

    Returns:
        MySQLConnection -- a client used to interact with the database server
    """
    mlwh_db_host = config.MLWH_DB_HOST
    mlwh_db_port = config.MLWH_DB_PORT
    if readonly:
        mlwh_db_username = config.MLWH_DB_RO_USER
        mlwh_db_password = config.MLWH_DB_RO_PASSWORD
    else:
        mlwh_db_username = config.MLWH_DB_RW_USER
        mlwh_db_password = config.MLWH_DB_RW_PASSWORD
    mlwh_db_db = config.MLWH_DB_DBNAME

    print(f"Attempting to connect to {mlwh_db_host} on port {mlwh_db_port}")
    print(f"mlwh_db_username = {mlwh_db_username}")
    print(f"mlwh_db_password = {mlwh_db_password}")

    mysql_conn = None
    try:
        mysql_conn = mysql.connect(
            host = mlwh_db_host,
            port = mlwh_db_port,
            username = mlwh_db_username,
            password = mlwh_db_password,
            database = mlwh_db_db,
        )
        if mysql_conn.is_connected():
            logger.debug('Connected to MySQL database')

    except Error as e:
        logger.error(f"Error connecting to MySQL database: {e}")

    finally:
        if mysql_conn is not None and mysql_conn.is_connected():
            return mysql_conn


def run_mysql_many_insert_on_duplicate_query(mysql_conn: MySQLConnection, values: []) -> None:
    if mysql_conn is None:
        return

    # TODO: values input needs to look like this:
    # values = [
    #     {
            # 'mongodb_id': ?,
            # 'root_sample_id': ?,
            # 'cog_uk_id': ?,
            # 'rna_id': ?,
            # 'plate_barcode': ?,
            # 'coordinate': ?,
            # 'result': ?,
            # 'date_tested_string': ?,
            # 'date_tested': ?,
            # 'source': ?,
            # 'lab_id': ?,
            # 'created_at_external': ?,
            # 'updated_at_external': ?,
    #     }
    # ]

    ## defining the Query
    # TODO: this could go as a constant somewhere
    sql_query = """
    INSERT INTO lighthouse_sample (
    mongodb_id,
    root_sample_id,
    cog_uk_id,
    rna_id,
    plate_barcode,
    coordinate,
    result,
    date_tested_string,
    date_tested,
    source,
    lab_id,
    created_at_external,
    updated_at_external
    )
    VALUES (
    %(mongodb_id)s,
    %(root_sample_id)s,
    %(cog_uk_id)s,
    %(rna_id)s,
    %(plate_barcode)s,
    %(coordinate)s,
    %(result)s,
    %(date_tested_string)s,
    %(date_tested)s,
    %(source)s,
    %(lab_id)s,
    %(created_at_external)s,
    %(updated_at_external)s
    )
    ON DUPLICATE KEY UPDATE
    plate_barcode=VALUES(plate_barcode),
    coordinate=VALUES(coordinate),
    date_tested_string=VALUES(date_tested_string),
    date_tested=VALUES(date_tested),
    source=VALUES(source),
    lab_id=VALUES(lab_id),
    created_at_external=VALUES(created_at_external),
    updated_at_external=VALUES(updated_at_external);
    """

    # mongodb_id, root_sample_id, cog_uk_id, rna_id, plate_barcode, coordinate, result, date_tested_string, ate_tested, source, lab_id, created_at_external, updated_at_external

    cursor = mysql_conn.cursor()

    ## executing the query with values
    cursor.executemany(sql_query, values)

    ## to make final output we have to run the 'commit()' method of the database object
    mysql_conn.commit()

    ## 'fetchall()' method fetches all the rows from the last executed statement
    # rows = cursor.fetchall()

    # fetch number of rows inserted/affected
    logger.debug(f"{cursor.rowcount} records inserted or updated in MLWH")

    # close the cursor
    cursor.close()

    # close the connection
    mysql_conn.close()
