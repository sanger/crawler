import logging
import time
from threading import Thread

from crawler.rabbit.async_consumer import AsyncConsumer

LOGGER = logging.getLogger(__name__)


class BackgroundConsumer(Thread):
    """This is an example consumer that will reconnect if the nested
    ExampleConsumer indicates that a reconnect is necessary.
    """

    def __init__(self, use_ssl, host, port, username, password, vhost, queue):
        super().__init__()
        self.name = type(self).__name__
        self.daemon = True
        self._reconnect_delay = 0
        self._use_ssl = use_ssl
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._vhost = vhost
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
            self._consumer_var = AsyncConsumer(
                self._use_ssl, self._host, self._port, self._username, self._password, self._vhost, self._queue
            )

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
