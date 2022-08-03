from typing import cast
from unittest.mock import MagicMock, patch

import pytest
from pika import PlainCredentials

from crawler.rabbit.basic_getter import BasicGetter
from crawler.types import RabbitServerDetails

DEFAULT_SERVER_DETAILS = RabbitServerDetails(
    uses_ssl=False, host="host", port=5672, username="username", password="password", vhost="vhost"
)


@pytest.fixture(autouse=True)
def logger():
    with patch("crawler.rabbit.basic_getter.LOGGER") as logger:
        yield logger


@pytest.fixture(autouse=True)
def message_logger():
    with patch("crawler.rabbit.basic_getter.MESSAGE_LOGGER") as message_logger:
        yield message_logger


@pytest.fixture(params=[[{}, {}, "body"]])
def channel(request):
    if request.param[1] is not None:
        properties = MagicMock()
        properties.headers = request.param[1]
    else:
        properties = None

    if request.param[2] is not None:
        body = request.param[2].encode()
    else:
        body = None

    channel = MagicMock()
    channel.basic_get.return_value = (request.param[0], properties, body)

    return (channel, *request.param)


@pytest.fixture(autouse=True)
def blocking_connection(channel):
    with patch("crawler.rabbit.basic_getter.BlockingConnection") as blocking_connection:
        blocking_connection.return_value.channel.return_value = channel[0]
        yield blocking_connection


@pytest.fixture
def subject():
    return BasicGetter(DEFAULT_SERVER_DETAILS)


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

    subject = BasicGetter(server_details)

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


def test_close_closes_the_connection(subject, blocking_connection):
    subject = BasicGetter(DEFAULT_SERVER_DETAILS)
    subject.get_message("")

    blocking_connection.return_value.close.assert_not_called()  # We haven't closed the connection

    subject.close()

    blocking_connection.return_value.close.assert_called_once()


@pytest.mark.parametrize("queue", ["", "queue"])
def test_get_message_gets_the_next_message(subject, blocking_connection, channel, logger, queue):
    channel = channel[0]

    subject = BasicGetter(DEFAULT_SERVER_DETAILS)
    subject.get_message(queue)

    logger.info.assert_called_once()
    assert "Fetching message" in logger.info.call_args.args[0]
    blocking_connection.assert_called_once_with(subject._connection_params)
    blocking_connection.return_value.channel.assert_called_once()
    channel.basic_get.assert_called_once()

    assert channel.basic_get.call_args.args[0] == queue
    assert channel.basic_get.call_args.kwargs["auto_ack"] is True


@pytest.mark.parametrize("queue", ["", "queue"])
def test_get_message_inside_a_context_closes_the_connection_automatically(blocking_connection, queue):
    with BasicGetter(DEFAULT_SERVER_DETAILS) as subject:
        subject.get_message(queue)

        blocking_connection.return_value.close.assert_not_called()  # We're still inside the context

    blocking_connection.return_value.close.assert_called_once()


@pytest.mark.parametrize("channel", [[None, None, None]], indirect=True)
def test_get_message_logs_correctly_when_no_message_fetched(subject, channel, logger):
    result = subject.get_message("")

    assert logger.info.call_count == 2
    assert "no message" in logger.info.call_args_list[1].args[0]

    assert result is None


@pytest.mark.parametrize("channel", [[{}, {}, "A body"], [{}, {"key": "value"}, "Another body"]], indirect=True)
def test_get_message_logs_and_returns_correct_message(subject, channel, logger, message_logger):
    _, _, headers, body = channel

    result = subject.get_message("")

    logger.info.assert_called_once()
    message_logger.info.assert_called_once()
    assert body in message_logger.info.call_args.args[0]

    assert result.headers == headers
    assert result.body.decode() == body
