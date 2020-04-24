import logging
from datetime import datetime
from typing import Any, Dict, List

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError
from pymongo.results import InsertOneResult

logger = logging.getLogger(__name__)


def create_mongo_client(config: Dict) -> MongoClient:
    mongo_host = config["MONGO_HOST"]
    mongo_port = config["MONGO_PORT"]

    logger.info(f"Connecting to {mongo_host} on port {mongo_port}")

    return MongoClient(config["MONGO_HOST"], int(config["MONGO_PORT"]))


def get_mongo_db(config: Dict, client: MongoClient) -> Database:
    db = config["MONGO_DB"]

    logger.debug(f"Get database: {db}")

    return client[config["MONGO_DB"]]


def get_mongo_collection(database: Database, collection_name: str) -> Collection:
    logger.debug(f"Get collection: {collection_name}")

    return database[collection_name]


def copy_collection(database: Database, collection: Collection) -> None:
    cloned_collection = f"{collection.name}_{datetime.now().strftime('%d%m%Y_%H%M')}"

    logger.debug(f"Copying {collection.name} to {cloned_collection}")

    current_docs = list(collection.find())

    result = database[cloned_collection].insert_many(current_docs)

    logger.debug(f"{len(result.inserted_ids)} documents copied to {cloned_collection}")


def create_import_record(
    status_collection: Collection, centre: Dict, docs_inserted: int, errors: List
) -> InsertOneResult:
    logger.debug(f"Creating the status record for {centre['name']}")

    status_doc = {
        "date": datetime.now().isoformat(timespec="seconds"),
        "centre_name": centre["name"],
        "csv_file_used": centre["sftp_file_name"],
        "number_of_records": docs_inserted,
        "errors": errors,
    }

    return status_collection.insert_one(status_doc)


def populate_collection(
    collection: Collection, documents: List[Dict[str, Any]], filter: str
) -> None:
    logger.debug(f"Populating/updating {collection.full_name} using '{filter}' as the filter")
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
