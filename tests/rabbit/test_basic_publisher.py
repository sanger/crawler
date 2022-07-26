from typing import cast
from unittest.mock import MagicMock, patch

import pika
import pytest
from pika import PlainCredentials
from pika.spec import PERSISTENT_DELIVERY_MODE

from crawler.constants import RABBITMQ_HEADER_KEY_SUBJECT, RABBITMQ_HEADER_KEY_VERSION
from crawler.rabbit.basic_publisher import BasicPublisher
from crawler.types import RabbitServerDetails

DEFAULT_SERVER_DETAILS = RabbitServerDetails(
    uses_ssl=False, host="host", port=5672, username="username", password="password", vhost="vhost"
)


@pytest.fixture
def logger():
    with patch("crawler.rabbit.basic_publisher.LOGGER") as logger:
        yield logger


@pytest.fixture
def channel():
    yield MagicMock()


@pytest.fixture
def blocking_connection(channel):
    with patch("crawler.rabbit.basic_publisher.BlockingConnection") as blocking_connection:
        blocking_connection.return_value.channel.return_value = channel
        yield blocking_connection


@pytest.fixture
def subject(blocking_connection):
    return BasicPublisher(DEFAULT_SERVER_DETAILS, 0, 5)


@pytest.mark.parametrize("uses_ssl", [True, False])
@pytest.mark.parametrize("host", ["", "host"])
@pytest.mark.parametrize("port", [8080, 5672])
@pytest.mark.parametrize("username", ["", "username"])
@pytest.mark.parametrize("password", ["", "password"])
@pytest.mark.parametrize("vhost", ["", "vhost"])
def test_constructor_creates_correct_connection_parameters(uses_ssl, host, port, username, password, vhost):
    server_details = RabbitServerDetails(
        uses_ssl=uses_ssl, host=host, port=port, username=username, password=password, vhost=vhost
    )

    subject = BasicPublisher(server_details, 0, 3)

    if server_details.uses_ssl:
        assert subject._connection_params.ssl_options is not None
    else:
        assert subject._connection_params.ssl_options is None

    assert subject._connection_params.host == server_details.host
    assert subject._connection_params.port == server_details.port
    assert subject._connection_params.virtual_host == server_details.vhost

    credentials = cast(PlainCredentials, subject._connection_params.credentials)
    assert credentials.username == server_details.username
    assert credentials.password == server_details.password


@pytest.mark.parametrize("exchange", ["", "exchange"])
@pytest.mark.parametrize("routing_key", ["", "routing_key"])
@pytest.mark.parametrize("body", ["".encode(), "body".encode()])
@pytest.mark.parametrize("schema_subject", ["", "subject"])
@pytest.mark.parametrize("schema_version", ["", "schema_version"])
def test_publish_message_publishes_the_message(
    subject, blocking_connection, channel, exchange, routing_key, body, schema_subject, schema_version
):
    subject.publish_message(exchange, routing_key, body, schema_subject, schema_version)

    blocking_connection.assert_called_once_with(subject._connection_params)
    blocking_connection.return_value.channel.assert_called_once()
    channel.confirm_delivery.assert_called_once()
    channel.basic_publish.assert_called_once()
    blocking_connection.return_value.close.assert_called_once()

    assert channel.basic_publish.call_args.kwargs["exchange"] == exchange
    assert channel.basic_publish.call_args.kwargs["routing_key"] == routing_key
    assert channel.basic_publish.call_args.kwargs["body"] == body

    message_properties = channel.basic_publish.call_args.kwargs["properties"]
    assert message_properties.delivery_mode == PERSISTENT_DELIVERY_MODE
    assert message_properties.headers[RABBITMQ_HEADER_KEY_SUBJECT] == schema_subject
    assert message_properties.headers[RABBITMQ_HEADER_KEY_VERSION] == schema_version


@pytest.mark.parametrize("error_count", [1, 2, 3, 4])
def test_publish_message_retries_when_needed(subject, channel, logger, error_count):
    unroutable_error = pika.exceptions.UnroutableError(["Boom!"])
    channel.basic_publish.side_effect = [unroutable_error] * error_count + [None]

    subject.publish_message("exchange", "routing_key", "body".encode(), "schema_subject", "schema_version")

    assert channel.basic_publish.call_count == error_count + 1

    logger.error.assert_called_once()
    log_message = logger.error.call_args.args[0]
    assert str(error_count) in log_message


@pytest.mark.parametrize("error_count", [5, 10, 100])
def test_publish_message_stops_retrying_after_max_retries(subject, channel, logger, error_count):
    unroutable_error = pika.exceptions.UnroutableError(["Boom!"])
    channel.basic_publish.side_effect = [unroutable_error] * error_count + [None]

    subject.publish_message("exchange", "routing_key", "body".encode(), "schema_subject", "schema_version")

    assert channel.basic_publish.call_count == 5

    logger.error.assert_called_once()
    log_message = logger.error.call_args.args[0]
    assert "NOT PUBLISHED" in log_message
