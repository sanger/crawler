import pytest
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from crawler.db.mongo import collection_exists, create_index, create_mongo_client, get_mongo_collection, get_mongo_db


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


def test_collection_exists_returns_correct_boolean(mongo_database):
    _, mongo_database = mongo_database

    centres_collection = mongo_database["centres"]
    centres_collection.insert_one({"test": "test"})

    assert collection_exists(mongo_database, "centres")
    assert not collection_exists(mongo_database, "something_else")


def test_create_index_creates_the_index_unique_not_specified(mongo_database):
    _, mongo_database = mongo_database

    centres_collection = mongo_database["centres"]
    centres_collection.insert_one({"test": "test"})

    assert "test_1" not in centres_collection.index_information()

    create_index(centres_collection, "test")

    assert "test_1" in centres_collection.index_information()
    index = centres_collection.index_information()["test_1"]
    assert index["key"] == [("test", 1)]
    assert index["unique"]


@pytest.mark.parametrize("unique", [(True), (False)])
def test_create_index_creates_the_index_with_unique_specified(mongo_database, unique):
    _, mongo_database = mongo_database

    centres_collection = mongo_database["centres"]
    centres_collection.insert_one({"test": "test"})

    assert "test_1" not in centres_collection.index_information()

    create_index(centres_collection, "test", unique=unique)

    assert "test_1" in centres_collection.index_information()
    index = centres_collection.index_information()["test_1"]
    assert index["key"] == [("test", 1)]
    assert ("unique" in index and index["unique"]) or not unique
