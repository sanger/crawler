import logging

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from crawler.types import Config

logger = logging.getLogger(__name__)


def create_mongo_client(config: Config) -> MongoClient:
    """Create a MongoClient with the given config parameters.

    Arguments:
        config {Config}: application config specifying host and port

    Returns:
        MongoClient: a client used to interact with the database server
    """
    logger.debug("Connecting to mongo")
    return MongoClient(config.MONGO_URI)


def get_mongo_db(config: Config, client: MongoClient) -> Database:
    """Get a handle on a mongodb database - remember that it is lazy and is only created when
    documents are added to a collection.

    Arguments:
        config {Config}: application config specifying the database
        client {MongoClient}: the client to use for the connection

    Returns:
        Database: a reference to the database in mongo
    """
    db = config.MONGO_DB

    logger.debug(f"Get database '{db}'")

    return client[db]


def get_mongo_collection(database: Database, collection_name: str) -> Collection:
    """Get a reference to a mongo collection from a database. A collection is created when documents
    are written to it.

    Arguments:
        database {Database}: the database to get a collection from
        collection_name {str}: the name of the collection to get/create

    Returns:
        Collection: a reference to the collection
    """
    logger.debug(f"Get collection '{collection_name}'")

    return database[collection_name]


def collection_exists(database: Database, collection_name: str) -> bool:
    """Identify whether the specified collection exists in MongoDB already.

    Arguments:
        database {Database}: the database to check for the collection's existance.
        collection_name {str}: the name of the collection to check the existance of.

    Returns:
        bool: True if the collection exists; otherwise False.
    """
    logger.debug(f"Checking whether collection exists '{collection_name}'")

    return collection_name in database.list_collection_names()


def create_index(collection: Collection, key: str, unique: bool = True) -> None:
    """Create an index for a specified key on a collection.

    Arguments:
        collection {Collection}: The collection to create an index on.
        key {str}: The key to create an index for.
        unique {bool}: Whether the index may only contain unique values.
    """
    logger.debug(f"Creating index '{key}' on '{collection.full_name}'")
    collection.create_index(key, unique=unique)
