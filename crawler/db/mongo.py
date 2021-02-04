import logging
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Iterator, List

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.results import InsertOneResult

from crawler.types import Config

logger = logging.getLogger(__name__)


def create_mongo_client(config: Config) -> MongoClient:
    """Create a MongoClient with the given config parameters.

    Arguments:
        config {Config} -- application config specifying host and port

    Returns:
        MongoClient -- a client used to interact with the database server
    """
    try:
        logger.debug("Connecting to mongo")

        mongo_uri = config.MONGO_URI

        return MongoClient(mongo_uri)
    except AttributeError:
        #  there is no MONGO_URI so try each config separately
        mongo_host = config.MONGO_HOST
        mongo_port = config.MONGO_PORT
        mongo_username = config.MONGO_USERNAME
        mongo_password = config.MONGO_PASSWORD
        mongo_db = config.MONGO_DB

        logger.debug(f"Connecting to {mongo_host} on port {mongo_port}")

        return MongoClient(
            host=mongo_host,
            port=mongo_port,
            username=mongo_username,
            password=mongo_password,
            authSource=mongo_db,
        )


def get_mongo_db(config: Config, client: MongoClient) -> Database:
    """Get a handle on a mongodb database - remember that it is lazy and is only created when
    documents are added to a collection.

    Arguments:
        config {Config} -- application config specifying the database
        client {MongoClient} -- the client to use for the connection

    Returns:
        Database -- a reference to the database in mongo
    """
    db = config.MONGO_DB

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
    logger.debug(f"Creating the import record for {centre['name']}")
    logger.info(f"{docs_inserted} documents inserted into sample collection")
    logger.debug(f"CSV file used: {file_name}")

    import_doc = {
        "date": datetime.utcnow(),  # https://pymongo.readthedocs.io/en/stable/examples/datetimes.html
        "centre_name": centre["name"],
        "csv_file_used": file_name,
        "number_of_records": docs_inserted,
        "errors": errors,
    }

    return import_collection.insert_one(import_doc)


def populate_collection(collection: Collection, documents: List[Dict[str, Any]], filter_field: str) -> None:
    """Populates a collection using the given documents. It uses the filter_field to replace any documents that match
    the filter and adds any new documents.

    Arguments:
        collection {Collection} -- collection to populate
        documents {List[Dict[str, Any]]} -- documents to populate the collection with
        filter_field {str} -- filter to search for matching documents
    """
    logger.debug(f"Populating/updating '{collection.full_name}' using '{filter_field}' as the filter")

    for document in documents:
        _ = collection.find_one_and_update({filter_field: document[filter_field]}, {"$set": document}, upsert=True)
