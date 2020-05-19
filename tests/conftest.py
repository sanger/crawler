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
    config, _ = get_config("crawler.config.test")
    return config


@pytest.fixture
def mongo_client(config):
    with create_mongo_client(config) as client:
        yield config, client


@pytest.fixture
def mongo_database(mongo_client):
    config, mongo_client = mongo_client
    return config, get_mongo_db(config, mongo_client)
