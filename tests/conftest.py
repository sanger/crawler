import logging
import logging.config
import shutil
import tempfile
import pytest
from unittest.mock import patch
from typing import Dict, List, Any
from crawler.constants import (
    COLLECTION_SAMPLES,
    COLLECTION_SAMPLES_HISTORY,
)

from crawler.db import create_mongo_client, get_mongo_db
from crawler.helpers import get_config
from crawler.db import get_mongo_collection

logger = logging.getLogger(__name__)
CONFIG, _ = get_config("crawler.config.test")
logging.config.dictConfig(CONFIG.LOGGING)  # type: ignore

from copy import deepcopy


@pytest.fixture
def config():
    return CONFIG


@pytest.fixture
def centre_with_added_columns():
    return CONFIG.EXTRA_COLUMN_CENTRE


@pytest.fixture
def mongo_client(config):
    with create_mongo_client(config) as client:
        yield config, client


@pytest.fixture
def mongo_database(mongo_client):
    config, mongo_client = mongo_client
    db = get_mongo_db(config, mongo_client)
    try:
        yield config, db
    # Drop the database after each test to ensure they are independent
    # A transaction may be more appropriate here, but that means significant
    # code changes, as 'sessions' need to be passed around. I'm also not
    # sure what version of mongo is being used in production.
    finally:
        mongo_client.drop_database(db)


@pytest.fixture
def testing_files_for_process(cleanup_backups):
    # Copy the test files to a new directory, as we expect run
    # to perform a clean up, and we don't want it cleaning up our
    # main copy of the data. We don't disable the clean up as:
    # 1) It also clears up the master files, which we'd otherwise need to handle
    # 2) It means we keep the tested process closer to the actual one
    _ = shutil.copytree("tests/files", "tmp/files", dirs_exist_ok=True)
    try:
        yield
    finally:
        # remove files https://docs.python.org/3/library/shutil.html#shutil.rmtree
        shutil.rmtree("tmp/files")
        # (_, _, files) = next(os.walk("tmp/files"))


TESTING_SAMPLES: List[Dict[str, str]] = [
    {
        "coordinate": "A01",
        "source": "test1",
        "Result": "Positive",
        "plate_barcode": "123",
        "released": True,
        "Root Sample ID": "MCM001",
    },
    {
        "coordinate": "B01",
        "source": "test1",
        "Result": "Negative",
        "plate_barcode": "123",
        "released": False,
        "Root Sample ID": "MCM002",
    },
    {
        "coordinate": "C01",
        "source": "test1",
        "Result": "Void",
        "plate_barcode": "123",
        "Root Sample ID": "MCM003",
    },
]


@pytest.fixture
def samples_collection_accessor(mongo_database):
    return get_mongo_collection(mongo_database[1], COLLECTION_SAMPLES)


@pytest.fixture
def samples_history_collection_accessor(mongo_database):
    return get_mongo_collection(mongo_database[1], COLLECTION_SAMPLES_HISTORY)


@pytest.fixture
def testing_samples(samples_collection_accessor):
    result = samples_collection_accessor.insert_many(TESTING_SAMPLES)
    samples = list(samples_collection_accessor.find({"_id": {"$in": result.inserted_ids}}))
    try:
        yield samples
    finally:
        samples_collection_accessor.delete_many({})


@pytest.fixture
def cleanup_backups():
    try:
        yield
    finally:
        shutil.rmtree("tmp/backups")


@pytest.fixture
def blacklist_for_centre(config):
    try:
        config.CENTRES[0]["file_names_to_ignore"] = ["AP_sanger_report_200503_2338.csv"]
        yield config
    finally:
        config.CENTRES[0]["file_names_to_ignore"] = []
