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

logger = logging.getLogger(__name__)


class CollectionError(Exception):
    """Raise to prevent safe_collection renaming the original collection"""

    pass


def create_mongo_client(config: ModuleType) -> MongoClient:
    """Create a MongoClient with the given config parameters.

    Arguments:
        config {ModuleType} -- application config specifying host and port

    Returns:
        MongoClient -- a client used to interact with the database server
    """
    try:
        logger.info(f"Connecting to mongo")
        mongo_uri = config.MONGO_URI  # type: ignore
        return MongoClient(mongo_uri)
    except AttributeError as e:
        #  there is no MONGO_URI so try each config separately
        logger.warning(e)

        mongo_host = config.MONGO_HOST  # type: ignore
        mongo_port = config.MONGO_PORT  # type: ignore
        mongo_username = config.MONGO_USERNAME  # type: ignore
        mongo_password = config.MONGO_PASSWORD  # type: ignore
        mongo_db = config.MONGO_DB  # type: ignore

        logger.info(f"Connecting to {mongo_host} on port {mongo_port}")

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


def rename_collection_with_suffix(collection: Collection, suffix: str = current_time()) -> None:
    """Renames a collection to a timestamped version of itself

    Arguments:
        collection {Collection} -- the collection to rename
        suffix {str} -- The suffix to add to the collection name
    """
    new_name = f"{collection.name}_{suffix}"

    rename_collection(collection, new_name)
    return None


def rename_collection(collection: Collection, new_name: str) -> None:
    """Renames a collection to the new name.

    Arguments:
        collection {Collection} -- the collection to rename
        new_name {str} -- the new name of the collection
    """
    logger.debug(f"Renaming '{collection.name}' to '{new_name}'")

    # get a list of all docs
    collection.rename(new_name)

    logger.debug(f"Collection renamed to: '{new_name}'")

    return None


@contextmanager
def samples_collection_accessor(
    database: Database, collection_name: str, timestamp: str
) -> Iterator[Collection]:
    logger.debug(f"Opening collection: {collection_name}")
    temporary_collection = get_mongo_collection(database, collection_name)

    yield temporary_collection


@contextmanager
def safe_collection(
    database: Database, collection_name: str, timestamp: str
) -> Iterator[Collection]:
    """
    Creates a context which yields a new temporary collection.
    If the context runs successfully, renames collection_name to collection_name_timestamp
    and renames the temporary collection to replace collection_name.
    If the context fails, the original collection is left in place. The temporary collection is not
    cleaned up to assist with debugging.

    Arguments:
        database {Database} -- the database of the collection to replace
        collection {Collection} -- the collection to replace
        timestamp {str} -- A timestamp to apply to the original and temporary collection names
    """
    temporary_collection_name = f"tmp_{collection_name}_{timestamp}"
    logger.debug(f"Generating temporary collection: {temporary_collection_name}")
    temporary_collection = get_mongo_collection(database, temporary_collection_name)

    try:
        yield temporary_collection
    except CollectionError:
        # We've seen a collection error. Log it and return to prevent the rename
        logger.error("Collection error: original collection left in place")
        return None
    except Exception:
        # We've seen a different exception. Log it (for reassurance) and re-raise
        logger.error("Exception: original collection left in place")
        raise

    # Mongo provides no simple way of checking if a collection exists
    if collection_name in database.list_collection_names():
        logger.debug("Successful, renaming original collection")
        original_collection = get_mongo_collection(database, collection_name)
        rename_collection_with_suffix(original_collection, timestamp)

    rename_collection(temporary_collection, collection_name)
    return None


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
            {filter_field: document[filter_field]},{'$set': document}, upsert=True
        )
