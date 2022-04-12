from unittest.mock import ANY, MagicMock, Mock, call, patch

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
    return AsyncConsumer(DEFAULT_SERVER_DETAILS, "queue", Mock())


@pytest.mark.parametrize("uses_ssl", [True, False])
def test_connect_provides_correct_parameters(mock_logger, uses_ssl):
    server_details = RabbitServerDetails(
        uses_ssl=uses_ssl, host="host", port=5672, username="username", password="password", vhost="vhost"
    )
    subject = AsyncConsumer(server_details, "queue", Mock())
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
        subject.on_connection_open(Mock())

    open_channel.assert_called_once()
    mock_logger.info.assert_called_once()


def test_on_connection_open_error_calls_reconnect(subject, mock_logger):
    error = Exception("An error")
    with patch("crawler.rabbit.async_consumer.AsyncConsumer.reconnect") as reconnect:
        subject.on_connection_open_error(Mock(), error)

    reconnect.assert_called_once()
    mock_logger.error.assert_called_once_with(ANY, error)


def test_on_connection_closed_sets_channel_to_none(subject):
    subject._connection = MagicMock()
    subject._channel = "Not none"
    subject.on_connection_closed(Mock(), "A reason")

    assert subject._channel is None


def test_on_connection_closed_stops_the_ioloop(subject):
    subject._connection = MagicMock()
    subject._closing = True
    subject.on_connection_closed(Mock(), "A reason")

    subject._connection.ioloop.stop.assert_called_once()


def test_on_connection_closed_reconnects_when_not_in_closing_state(subject, mock_logger):
    subject._connection = MagicMock()
    subject._closing = False
    reason = "A reason"
    with patch("crawler.rabbit.async_consumer.AsyncConsumer.reconnect") as reconnect:
        subject.on_connection_closed(Mock(), reason)

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


@pytest.mark.parametrize("prefetch_count", [1, 5, 10])
def test_on_basic_qos_ok_calls_start_consuming(subject, mock_logger, prefetch_count):
    subject._prefetch_count = prefetch_count
    with patch("crawler.rabbit.async_consumer.AsyncConsumer.start_consuming") as start_consuming:
        subject.on_basic_qos_ok(Mock())

    start_consuming.assert_called_once()
    mock_logger.info.assert_called_once_with(ANY, prefetch_count)


def test_start_consuming_logs_when_no_channel(subject, mock_logger):
    subject._channel = None
    subject.start_consuming()

    mock_logger.error.assert_called_once()


def test_start_consuming_takes_necessary_actions(subject, mock_logger):
    # Test objects
    test_tag = "Test tag"
    test_queue = "queue.name"

    # Arrange
    subject._queue = test_queue
    subject._channel = MagicMock()
    subject._channel.basic_consume = Mock(return_value=test_tag)
    subject._consumer_tag = None
    subject.was_consuming = False
    subject._consuming = False

    # Act
    with patch("crawler.rabbit.async_consumer.AsyncConsumer.add_on_cancel_callback") as add_callback:
        subject.start_consuming()

    # Assert
    mock_logger.info.assert_called_once()
    add_callback.assert_called_once()
    subject._channel.basic_consume.assert_called_once_with(test_queue, ANY)
    assert subject._consumer_tag == test_tag
    assert subject.was_consuming is True
    assert subject._consuming is True


def test_add_on_cancel_callback_calls_the_channel_method(subject, mock_logger):
    subject._channel = MagicMock()
    subject.add_on_cancel_callback()

    subject._channel.add_on_cancel_callback.assert_called_once()
    mock_logger.info.assert_called_once()


def test_add_on_cancel_callback_logs_when_no_channel(subject, mock_logger):
    subject._channel = None
    subject.add_on_cancel_callback()

    mock_logger.error.assert_called_once()


def test_on_consumer_cancelled_logs(subject, mock_logger):
    method_frame = Mock()
    subject.on_consumer_cancelled(method_frame)

    mock_logger.info.assert_called_once_with(ANY, method_frame)


def test_on_consumer_cancelled_calls_channel_close(subject, mock_logger):
    subject._channel = MagicMock()
    subject.on_consumer_cancelled(Mock())

    subject._channel.close.assert_called_once()


def test_on_message_passes_relevant_info_to_process_message(subject, mock_logger):
    subject._process_message = MagicMock()

    # Arrange arguments
    channel = MagicMock()

    delivery_tag = "Test tag"
    basic_deliver = MagicMock()
    basic_deliver.delivery_tag = delivery_tag

    app_id = "Test app ID"
    headers = {"header1": "value1"}
    properties = MagicMock()
    properties.app_id = app_id
    properties.headers = headers

    body = "A message body"

    # Act on main function
    subject.on_message(channel, basic_deliver, properties, body)

    # Assert main function
    mock_logger.info.assert_called_once_with(ANY, delivery_tag, app_id, body)
    subject._process_message.assert_called_once_with(headers, body, ANY)
    callback_function = subject._process_message.call_args[0][2]

    # Act and assert on callback function with success
    callback_function(True)
    mock_logger.info.assert_called_with(ANY, delivery_tag)
    channel.basic_ack.assert_called_once_with(delivery_tag)

    # Act and assert on callback function with failure
    callback_function(False)
    mock_logger.info.assert_called_with(ANY, delivery_tag)
    channel.basic_nack.assert_called_once_with(delivery_tag, requeue=False)


def test_stop_consuming_calls_the_channel_method(subject, mock_logger):
    subject._channel = MagicMock()
    subject._consumer_tag = Mock()
    subject.stop_consuming()

    subject._channel.basic_cancel.assert_called_once_with(subject._consumer_tag, ANY)
    mock_logger.info.assert_called_once()


def test_on_cancelok_calls_close_channel_method(subject, mock_logger):
    subject._channel = MagicMock()
    subject._consuming = True
    userdata = Mock()

    with patch("crawler.rabbit.async_consumer.AsyncConsumer.close_channel") as close_channel:
        subject.on_cancelok(Mock(), userdata)

    assert subject._consuming is False
    mock_logger.info.assert_called_once_with(ANY, userdata)
    close_channel.assert_called_once()


def test_close_channel_calls_the_channel_method(subject, mock_logger):
    subject._channel = MagicMock()
    subject.close_channel()

    subject._channel.close.assert_called_once()
    mock_logger.info.assert_called_once()


def test_close_channel_logs_when_no_channel(subject, mock_logger):
    subject._channel = None
    subject.close_channel()

    mock_logger.error.assert_called_once()


def test_run_starts_the_ioloop_when_connection_created(subject):
    subject._connection = None
    test_connection = MagicMock()
    with patch("crawler.rabbit.async_consumer.AsyncConsumer.connect", return_value=test_connection):
        subject.run()

    assert subject._connection == test_connection
    test_connection.ioloop.start.assert_called_once()


def test_run_logs_error_when_connection_not_created(subject, mock_logger):
    subject._connection = None
    with patch("crawler.rabbit.async_consumer.AsyncConsumer.connect", return_value=None):
        subject.run()

    mock_logger.error.assert_called_once()


def test_stop_logs_process(subject, mock_logger):
    subject._closing = False
    with patch("crawler.rabbit.async_consumer.AsyncConsumer.stop_consuming"):
        subject.stop()

    mock_logger.info.assert_has_calls([call("Stopping"), call("Stopped")])


def test_stop_takes_correct_actions_when_consuming(subject):
    subject._closing = False
    subject._consuming = True
    subject._connection = MagicMock()
    with patch("crawler.rabbit.async_consumer.AsyncConsumer.stop_consuming") as stop_consuming:
        subject.stop()

    stop_consuming.assert_called_once()
    subject._connection.ioloop.stop.assert_not_called()
    subject._connection.ioloop.start.assert_called_once()


def test_stop_takes_correct_actions_when_not_consuming(subject):
    subject._closing = False
    subject._consuming = False
    subject._connection = MagicMock()
    with patch("crawler.rabbit.async_consumer.AsyncConsumer.stop_consuming") as stop_consuming:
        subject.stop()

    stop_consuming.assert_not_called()
    subject._connection.ioloop.stop.assert_called_once()
    subject._connection.ioloop.start.assert_not_called()


def test_stop_does_nothing_if_already_closing(subject, mock_logger):
    subject._closing = True
    with patch("crawler.rabbit.async_consumer.AsyncConsumer.stop_consuming") as stop_consuming:
        subject.stop()

    stop_consuming.assert_not_called()
    mock_logger.info.assert_not_called()
