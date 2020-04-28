import logging
from datetime import datetime
from typing import Any, Dict, List

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError
from pymongo.results import InsertOneResult

logger = logging.getLogger(__name__)


def create_mongo_client(config: Dict[str, str]) -> MongoClient:
    """Create a MongoClient with the given config parameters.

    Arguments:
        config {Dict[str, str]} -- application config specifying host and port

    Returns:
        MongoClient -- a client used to interact with the database server
    """
    mongo_host = config["MONGO_HOST"]
    mongo_port = config["MONGO_PORT"]

    logger.info(f"Connecting to {mongo_host} on port {mongo_port}")

    return MongoClient(config["MONGO_HOST"], int(config["MONGO_PORT"]))


def get_mongo_db(config: Dict[str, str], client: MongoClient) -> Database:
    """Get a handle on a mongodb database - remember that it is lazy and is only created when
    documents are added to a collection.

    Arguments:
        config {Dict[str, str]} -- application config specifying the database
        client {MongoClient} -- the client to use for the connection

    Returns:
        Database -- a reference to the database in mongo
    """
    db = config["MONGO_DB"]

    logger.debug(f"Get database '{db}'")

    return client[config["MONGO_DB"]]


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


def copy_collection(database: Database, collection: Collection) -> None:
    """Copy a collection to a timestamped version of itself.

    Arguments:
        database {Database} -- the database of the collection to copy
        collection {Collection} -- the collection to copy
    """
    cloned_collection = f"{collection.name}_{datetime.now().strftime('%d%m%Y_%H%M')}"

    logger.debug(f"Copying '{collection.name}' to '{cloned_collection}'")

    current_docs = list(collection.find())

    result = database[cloned_collection].insert_many(current_docs)

    logger.debug(f"{len(result.inserted_ids)} documents copied to '{cloned_collection}'")


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


def populate_collection(
    collection: Collection, documents: List[Dict[str, Any]], filter: str
) -> None:
    """Populates a collection using the given documents. It uses the filter to replace any documents
    that match the filter and adds any new documents.

    Arguments:
        collection {Collection} -- collection to populate
        documents {List[Dict[str, Any]]} -- documents to populate the collection with
        filter {str} -- filter to search for matching documents
    """
    logger.debug(f"Populating/updating '{collection.full_name}' using '{filter}' as the filter")

    for document in documents:
        try:
            temp_doc = dict(document)  # insert_one() adds an _id field
            _ = collection.insert_one(document)
        except DuplicateKeyError:
            try:
                _ = collection.find_one_and_replace({filter: document[filter]}, temp_doc)
            except KeyError:
                logger.exception(f"Cannot update {collection.full_name}")
            continue
