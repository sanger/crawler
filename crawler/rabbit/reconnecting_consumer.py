import logging
import time

from crawler.rabbit.async_consumer import AsyncConsumer

LOGGER = logging.getLogger(__name__)


class ReconnectingConsumer(object):
    """This is an example consumer that will reconnect if the nested
    ExampleConsumer indicates that a reconnect is necessary.
    """

    def __init__(self, amqp_url, queue):
        self._reconnect_delay = 0
        self._amqp_url = amqp_url
        self._queue = queue
        self._consumer_var = None

    def run(self):
        while True:
            try:
                self._consumer.run()
            except KeyboardInterrupt:
                self._consumer.stop()
                break
            self._maybe_reconnect()

    @property
    def _consumer(self):
        if self._consumer_var is None:
            self._consumer_var = AsyncConsumer(self._amqp_url, self._queue)

        return self._consumer_var

    def _maybe_reconnect(self):
        if self._consumer.should_reconnect:
            self._consumer.stop()
            reconnect_delay = self._get_reconnect_delay()
            LOGGER.info("Reconnecting after %d seconds", reconnect_delay)
            time.sleep(reconnect_delay)
            self._consumer_var = None

    def _get_reconnect_delay(self):
        if self._consumer.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay
