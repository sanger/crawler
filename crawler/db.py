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
from mysql.connector.connection_cext import CMySQLConnection
from mysql.connector import Error
from crawler.sql_queries import SQL_MLWH_MULTIPLE_INSERT

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

def create_mysql_connection(config: ModuleType, readonly = True) -> CMySQLConnection:
    """Create a CMySQLConnection with the given config parameters.

    Arguments:
        config {ModuleType} -- application config specifying database details

    Returns:
        CMySQLConnection -- a client used to interact with the database server
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

    mysql_conn = None
    try:
        mysql_conn = mysql.connect(
            host = mlwh_db_host,
            port = mlwh_db_port,
            username = mlwh_db_username,
            password = mlwh_db_password,
            database = mlwh_db_db,
            # whether to use pure python or the C extension.
            # This is the default, but specify it so more predictable
            use_pure = False
        )
        if mysql_conn is not None:
            if mysql_conn.is_connected():
                logger.debug('MySQL Connection Successful')
            else:
                logger.error('MySQL Connection Failed')

    except Error as e:
        logger.error(f"Exception on connecting to MySQL database: {e}")

    return mysql_conn

# def run_mysql_many_insert_on_duplicate_query(mysql_conn: CMySQLConnection, values: List[Dict[str, str]]) -> None:
#     """Writes the values from the samples into the MLWH.

#     Arguments:
#         mysql_conn {CMySQLConnection} -- a client used to interact with the database server
#         values {List[Dict[str, str]]} -- array of value hashes representing documents inserted into the Mongo DB
#     """

#     ## defining the Query
#     sql_query = SQL_MLWH_MULTIPLE_INSERT

#     ## fetch the cursor from the DB connection
#     cursor = mysql_conn.cursor()
#     try:
#         try:
#             ## executing the query with values
#             logger.debug(f"Attempting to insert or update {len(values)} rows in the MLWH")
#             cursor.executemany(sql_query, values)
#         except:
#             logger.debug(f"Database transaction failed. Rolling back...")
#             mysql_conn.rollback()
#             raise # add specific info
#         else:
#             logger.debug('Database transaction succeeded. Committing changes to database.')
#             mysql_conn.commit()
#             logger.debug('Changes have been committed to the database.')
#             # fetch number of rows inserted/affected - not easy to interpret:
#             # reports 1 per inserted row,
#             # 2 per updated existing row,
#             # and 0 per unchanged existing row
#             logger.debug(f"{cursor.rowcount} rows affected in MLWH. (Note: each updated row increase the count by 2, instead of 1)")
#     except:
#         logger.debug('Database committing errored')
#         # log a critical error
#     finally:
#         # close the cursor
#         cursor.close()

#         # close the connection
#         mysql_conn.close()

def run_mysql_many_insert_on_duplicate_query(mysql_conn: CMySQLConnection, values: List[Dict[str, str]]) -> None:
    """Writes the values from the samples into the MLWH.

    Arguments:
        mysql_conn {CMySQLConnection} -- a client used to interact with the database server
        values {List[Dict[str, str]]} -- array of value hashes representing documents inserted into the Mongo DB
    """

    ## defining the Query
    sql_query = SQL_MLWH_MULTIPLE_INSERT

    ## fetch the cursor from the DB connection
    cursor = mysql_conn.cursor()

    try:
        ## executing the query with values
        logger.debug(f"Attempting to insert or update {len(values)} rows in the MLWH database")
        cursor.executemany(sql_query, values)

        logger.debug('Committing changes to MLWH database.')
        mysql_conn.commit()

        # fetch number of rows inserted/affected - not easy to interpret:
        # reports 1 per inserted row,
        # 2 per updated existing row,
        # and 0 per unchanged existing row
        logger.debug(f"{cursor.rowcount} rows affected in MLWH. (Note: each updated row increase the count by 2, instead of 1)")
    except:
        logger.error('MLWH database transaction failed')
        logger.critical('MLWH database transaction failed')
        raise
    finally:
        # close the cursor
        cursor.close()

        # close the connection
        mysql_conn.close()