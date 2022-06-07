import logging
import time
from threading import Thread

from crawler.rabbit.async_consumer import AsyncConsumer

LOGGER = logging.getLogger(__name__)


class BackgroundConsumer(Thread):
    """This is a RabbitMQ consumer that runs in a background thread and will reconnect
    after a time delay if the AsyncConsumer indicates that a reconnect is necessary.
    """

    def __init__(self, server_details, queue, process_message):
        super().__init__()
        self.name = type(self).__name__
        self.daemon = True
        self._reconnect_delay = 0
        self._server_details = server_details
        self._queue = queue
        self._process_message = process_message
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
    def is_healthy(self):
        return self._consumer.is_healthy

    @property
    def _consumer(self):
        if self._consumer_var is None:
            self._consumer_var = AsyncConsumer(self._server_details, self._queue, self._process_message)

        return self._consumer_var

    def _maybe_reconnect(self):
        if self._consumer.should_reconnect:
            self._consumer.stop()
            reconnect_delay = self._get_reconnect_delay()
            LOGGER.info("Reconnecting after %d seconds", reconnect_delay)
            time.sleep(reconnect_delay)
            self._consumer_var = None

    def _get_reconnect_delay(self):
        if self._consumer.had_transient_error:
            self._reconnect_delay = 30
        elif self._consumer.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay
