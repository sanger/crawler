import logging
import logging.config

import pytest

from crawler.db import create_mongo_client, get_mongo_db
from crawler.helpers import get_config

logger = logging.getLogger(__name__)
CONFIG, _ = get_config("crawler.config.test")
logging.config.dictConfig(CONFIG.LOGGING)  # type: ignore


@pytest.fixture
def config():
    return CONFIG


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
