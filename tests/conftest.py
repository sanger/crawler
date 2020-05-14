import logging
import logging.config

import pytest

from crawler.config.logging import LOGGING_CONF
from crawler.db import create_mongo_client, get_mongo_db
from crawler.helpers import get_config

logging.config.dictConfig(LOGGING_CONF)
logger = logging.getLogger(__name__)


@pytest.fixture
def config():
    return get_config("crawler.config.test")


@pytest.fixture
def mongo_client(config):
    return config, create_mongo_client(config)


@pytest.fixture
def mongo_database(mongo_client):
    config, mongo_client = mongo_client
    return config, get_mongo_db(config, mongo_client)
