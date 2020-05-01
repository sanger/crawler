import logging
import logging.config

import pytest

from crawler.config.logging import LOGGING_CONF
from crawler.config_helpers import get_centre_details, get_config
from crawler.db import create_mongo_client, get_mongo_db

TEST_CONFIG = {
    "CENTRE_DETAILS_FILE_PATH": "tests/config/centre_details.json",
    "MONGO_DB": "crawlerTestDB",
    "MONGO_PASSWORD": "",
    "MONGO_PORT": "27017",
    "MONGO_USER": "",
    "SFTP_PASSWORD": "pass",
    "SFTP_PORT": "22",
    "SFTP_USER": "foo",
    "SLACK_API_TOKEN": "xoxb",
    "SLACK_CHANNEL_ID": "C",
}

logging.config.dictConfig(LOGGING_CONF)
logger = logging.getLogger(__name__)


@pytest.fixture
def config():
    return get_config(TEST_CONFIG)


@pytest.fixture
def centre_details(config):
    return get_centre_details(config)


@pytest.fixture
def mongo_client(config):
    return config, create_mongo_client(config)


@pytest.fixture
def mongo_database(mongo_client):
    config, mongo_client = mongo_client
    return get_mongo_db(config, mongo_client)
