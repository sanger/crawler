import pytest

from crawler.config.centres import get_centres_config
from crawler.constants import CENTRE_KEY_DATA_SOURCE


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
