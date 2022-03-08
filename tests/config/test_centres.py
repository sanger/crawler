import pytest

from crawler.config.centres import get_centres_config
from crawler.constants import CENTRE_KEY_DATA_SOURCE, CENTRE_KEY_NAME


def test_get_centres_config_no_data_source_specified(test_data_source_centres):
    _, config = test_data_source_centres

    actual = get_centres_config(config)

    assert len(actual) == 2


@pytest.mark.parametrize(
    "requested,actual_data_source",
    [
        ("sftp", "SFTP"),
        ("SFTP", "SFTP"),
        ("SfTp", "SFTP"),
        ("rabbitmq", "RabbitMQ"),
        ("RABBITMQ", "RabbitMQ"),
        ("RaBbItMq", "RabbitMQ"),
    ],
)
def test_get_centres_config_with_data_source(test_data_source_centres, requested, actual_data_source):
    _, config = test_data_source_centres

    actual = get_centres_config(config, requested)

    assert len(actual) == 1
    assert actual[0][CENTRE_KEY_DATA_SOURCE] == actual_data_source


def test_get_centres_config_only_populates_database_once(centres_collection_accessor, config):
    # Check the initial state is no documents.
    assert centres_collection_accessor.count_documents({}) == 0

    get_centres_config(config)

    # Now the database has all the centre documents from the configuration file.
    assert centres_collection_accessor.count_documents({}) == 12

    # Delete a document and ensure it isn't added back.
    centres_collection_accessor.delete_one({CENTRE_KEY_NAME: "Alderley"})
    get_centres_config(config)
    assert centres_collection_accessor.count_documents({}) == 11

    # Delete all the documents and ensure they aren't added back.
    centres_collection_accessor.delete_many({})
    get_centres_config(config)
    assert centres_collection_accessor.count_documents({}) == 0

    # Drop the whole collection and ensure it is added once more.
    centres_collection_accessor.drop()
    get_centres_config(config)
    assert centres_collection_accessor.count_documents({}) == 12
