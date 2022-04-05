from unittest.mock import MagicMock, Mock, patch

import pytest
from more_itertools import side_effect

from crawler.rabbit.background_consumer import BackgroundConsumer


def test_init_sets_the_correct_name():
    subject = BackgroundConsumer(False, "host", 5672, "username", "password", "vhost", "queue")
    assert subject.name == "BackgroundConsumer"


def test_init_sets_daemon_thread_true():
    subject = BackgroundConsumer(False, "host", 5672, "username", "password", "vhost", "queue")
    assert subject.daemon == True


@pytest.mark.parametrize("use_ssl", [True, False])
@pytest.mark.parametrize("host", ["", "host"])
@pytest.mark.parametrize("port", [8080, 5672])
@pytest.mark.parametrize("username", ["", "username"])
@pytest.mark.parametrize("password", ["", "password"])
@pytest.mark.parametrize("vhost", ["", "vhost"])
@pytest.mark.parametrize("queue", ["", "queue"])
def test_consumer_is_passed_correct_parameters(use_ssl, host, port, username, password, vhost, queue):
    subject = BackgroundConsumer(use_ssl, host, port, username, password, vhost, queue)

    with patch("crawler.rabbit.background_consumer.AsyncConsumer.__init__", return_value=None) as async_consumer_init:
        # Initiate creation of the AsyncConsumer
        subject._consumer

    async_consumer_init.assert_called_once_with(use_ssl, host, port, username, password, vhost, queue)


def test_run_starts_consumer_and_stops_on_keyboard_interrupt():
    subject = BackgroundConsumer(False, "host", 5672, "username", "password", "vhost", "queue")

    with patch("crawler.rabbit.background_consumer.AsyncConsumer") as consumer:
        consumer.return_value.run.side_effect = KeyboardInterrupt()
        subject.run()

    consumer.return_value.run.assert_called_once()
    consumer.return_value.stop.assert_called_once()


def test_maybe_reconnect_sleeps_longer_each_time():
    subject = BackgroundConsumer(False, "host", 5672, "username", "password", "vhost", "queue")

    with patch("crawler.rabbit.background_consumer.time.sleep") as sleep_func:
        with patch("crawler.rabbit.background_consumer.AsyncConsumer") as consumer:
            consumer.return_value.was_consuming = False
            consumer.return_value.should_reconnect = True

            subject._maybe_reconnect()
            sleep_func.assert_called_with(1)
            subject._maybe_reconnect()
            sleep_func.assert_called_with(2)
            subject._maybe_reconnect()
            sleep_func.assert_called_with(3)

            subject._reconnect_delay = 28
            subject._maybe_reconnect()
            sleep_func.assert_called_with(29)
            subject._maybe_reconnect()
            sleep_func.assert_called_with(30)
            subject._maybe_reconnect()  # Maximum delay is 30 seconds
            sleep_func.assert_called_with(30)

            assert consumer.return_value.stop.call_count == 6


def test_maybe_reconnect_sleeps_zero_seconds_if_consumer_was_consuming():
    subject = BackgroundConsumer(False, "host", 5672, "username", "password", "vhost", "queue")

    with patch("crawler.rabbit.background_consumer.time.sleep") as sleep_func:
        with patch("crawler.rabbit.background_consumer.AsyncConsumer") as consumer:
            consumer.return_value.was_consuming = True
            consumer.return_value.should_reconnect = True

            for x in range(5):
                subject._maybe_reconnect()
                sleep_func.assert_called_with(0)

            assert consumer.return_value.stop.call_count == 5
