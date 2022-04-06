from unittest.mock import ANY, MagicMock, Mock, patch

import pytest

from crawler.rabbit.async_consumer import AsyncConsumer
from crawler.types import RabbitServerDetails

DEFAULT_SERVER_DETAILS = RabbitServerDetails(
    uses_ssl=False, host="host", port=5672, username="username", password="password", vhost="vhost"
)


@pytest.fixture
def mock_logger():
    with patch("crawler.rabbit.async_consumer.LOGGER") as logger:
        yield logger


@pytest.fixture
def subject():
    return AsyncConsumer(DEFAULT_SERVER_DETAILS, "queue")


@pytest.mark.parametrize("uses_ssl", [True, False])
def test_connect_provides_correct_parameters(mock_logger, uses_ssl):
    server_details = RabbitServerDetails(
        uses_ssl=uses_ssl, host="host", port=5672, username="username", password="password", vhost="vhost"
    )
    subject = AsyncConsumer(server_details, "queue")
    select_connection = subject.connect()
    select_connection.close()  # Don't want async callbacks that will log during other tests

    parameters = select_connection.params
    if server_details.uses_ssl:
        assert parameters.ssl_options is not None
    else:
        assert parameters.ssl_options is None

    assert parameters.host == server_details.host
    assert parameters.port == server_details.port
    assert parameters.credentials.username == server_details.username
    assert parameters.credentials.password == server_details.password
    assert parameters.virtual_host == server_details.vhost
    mock_logger.info.assert_called_once()


def test_close_connection_sets_consuming_false(subject, mock_logger):
    subject._consuming = True
    subject.close_connection()

    assert subject._consuming is False
    mock_logger.info.assert_called_once()


def test_close_connection_calls_close_on_connection(subject, mock_logger):
    subject._connection = MagicMock()
    subject._connection.is_closing = False
    subject._connection.is_closed = False
    subject.close_connection()

    subject._connection.close.assert_called_once()
    mock_logger.info.assert_called_once()


def test_on_connection_open_calls_open_channel(subject, mock_logger):
    with patch("crawler.rabbit.async_consumer.AsyncConsumer.open_channel") as open_channel:
        subject.on_connection_open(None)

    open_channel.assert_called_once()
    mock_logger.info.assert_called_once()


def test_on_connection_open_error_calls_reconnect(subject, mock_logger):
    error = Exception("An error")
    with patch("crawler.rabbit.async_consumer.AsyncConsumer.reconnect") as reconnect:
        subject.on_connection_open_error(None, error)

    reconnect.assert_called_once()
    mock_logger.error.assert_called_once_with(ANY, error)


def test_on_connection_closed_sets_channel_to_none(subject):
    subject._connection = MagicMock()
    subject._channel = "Not none"
    subject.on_connection_closed(None, "A reason")

    assert subject._channel is None


def test_on_connection_closed_stops_the_ioloop(subject):
    subject._connection = MagicMock()
    subject._closing = True
    subject.on_connection_closed(None, "A reason")

    subject._connection.ioloop.stop.assert_called_once()


def test_on_connection_closed_reconnects_when_not_in_closing_state(subject, mock_logger):
    subject._connection = MagicMock()
    subject._closing = False
    reason = "A reason"
    with patch("crawler.rabbit.async_consumer.AsyncConsumer.reconnect") as reconnect:
        subject.on_connection_closed(None, reason)

    reconnect.assert_called_once()
    mock_logger.warning.assert_called_once_with(ANY, reason)


def test_reconnect_prepares_for_reconnection(subject):
    subject.should_reconnect = False
    with patch("crawler.rabbit.async_consumer.AsyncConsumer.stop") as stop:
        subject.reconnect()

    assert subject.should_reconnect is True
    stop.assert_called_once()


def test_open_channel_calls_the_connection_method(subject, mock_logger):
    subject._connection = MagicMock()
    subject.open_channel()

    subject._connection.channel.assert_called_once()
    mock_logger.info.assert_called_once()


def test_open_channel_logs_when_no_connection(subject, mock_logger):
    subject._connection = None
    subject.open_channel()

    mock_logger.error.assert_called_once()


def test_on_channel_open_sets_the_channel_and_calls_follow_up_methods(subject, mock_logger):
    subject._channel = None
    fake_channel = Mock()
    with patch("crawler.rabbit.async_consumer.AsyncConsumer.add_on_channel_close_callback") as add_callback:
        with patch("crawler.rabbit.async_consumer.AsyncConsumer.set_qos") as set_qos:
            subject.on_channel_open(fake_channel)

    mock_logger.info.assert_called_once()
    assert subject._channel == fake_channel
    add_callback.assert_called_once()
    set_qos.assert_called_once()


def test_add_on_channel_close_callback_calls_the_channel_method(subject, mock_logger):
    subject._channel = MagicMock()
    subject.add_on_channel_close_callback()

    subject._channel.add_on_close_callback.assert_called_once()
    mock_logger.info.assert_called_once()


def test_add_on_channel_close_callback_logs_when_no_channel(subject, mock_logger):
    subject._channel = None
    subject.add_on_channel_close_callback()

    mock_logger.error.assert_called_once()


def test_on_channel_closed_calls_close_connection(subject, mock_logger):
    channel = "A channel"
    reason = "A reason"
    with patch("crawler.rabbit.async_consumer.AsyncConsumer.close_connection") as close_connection:
        subject.on_channel_closed(channel, reason)

    close_connection.assert_called_once()
    mock_logger.warning.assert_called_once_with(ANY, channel, reason)


@pytest.mark.parametrize("prefetch_count", [1, 5, 10])
def test_set_qos_applies_prefetch_count_to_channel(subject, prefetch_count):
    subject._prefetch_count = prefetch_count
    subject._channel = MagicMock()
    subject.set_qos()

    subject._channel.basic_qos.assert_called_once_with(prefetch_count=prefetch_count, callback=ANY)
