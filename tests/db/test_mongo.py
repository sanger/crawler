from datetime import datetime

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from crawler.db.mongo import create_import_record, create_mongo_client, get_mongo_collection, get_mongo_db
from crawler.helpers.logging_helpers import LoggingCollection

from crawler.constants import (
    FIELD_MONGODB_ID,
)


def test_create_mongo_client(config):
    assert type(create_mongo_client(config)) == MongoClient


def test_get_mongo_db(mongo_client):
    config, mongo_client = mongo_client

    assert type(get_mongo_db(config, mongo_client)) == Database


def test_get_mongo_collection(mongo_database):
    _, mongo_database = mongo_database
    collection_name = "test_collection"
    test_collection = get_mongo_collection(mongo_database, collection_name)

    assert type(test_collection) == Collection
    assert test_collection.name == collection_name


def test_create_import_record(freezer, mongo_database):
    config, mongo_database = mongo_database
    import_collection = mongo_database["imports"]

    docs = [{"x": 1}, {"y": 2}, {"z": 3}]
    error_collection = LoggingCollection()
    error_collection.add_error("TYPE 4", "error1")
    error_collection.add_error("TYPE 5", "error2")

    for centre in config.CENTRES:
        now = datetime.utcnow()
        result = create_import_record(
            import_collection, centre, len(docs), "test", error_collection.get_messages_for_import()
        )
        import_doc = import_collection.find_one({FIELD_MONGODB_ID: result.inserted_id})

        assert import_doc["date"].replace(microsecond=0) == now.replace(microsecond=0)
        assert import_doc["centre_name"] == centre["name"]
        assert import_doc["csv_file_used"] == "test"
        assert import_doc["number_of_records"] == len(docs)
        assert import_doc["errors"] == error_collection.get_messages_for_import()
