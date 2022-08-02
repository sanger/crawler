import logging
import os
import ssl
from dataclasses import dataclass
from typing import Optional

from pika import BlockingConnection, ConnectionParameters, PlainCredentials, SSLOptions

from crawler.constants import LOGGER_NAME_RABBIT_MESSAGES
from crawler.types import RabbitServerDetails

LOGGER = logging.getLogger(__name__)
MESSAGE_LOGGER = logging.getLogger(LOGGER_NAME_RABBIT_MESSAGES)


@dataclass
class FetchedMessage:
    headers: dict
    body: bytes


class BasicGetter:
    def __init__(self, server_details: RabbitServerDetails):
        credentials = PlainCredentials(server_details.username, server_details.password)
        self._connection_params = ConnectionParameters(
            host=server_details.host,
            port=server_details.port,
            virtual_host=server_details.vhost,
            credentials=credentials,
        )

        if server_details.uses_ssl:
            cafile = os.getenv("REQUESTS_CA_BUNDLE")
            ssl_context = ssl.create_default_context(cafile=cafile)
            self._connection_params.ssl_options = SSLOptions(ssl_context)

        self.__channel = None

    def __del__(self):
        if self.__channel is not None:
            self.__channel.connection.close()

    @property
    def _channel(self):
        if self.__channel is None:
            connection = BlockingConnection(self._connection_params)
            self.__channel = connection.channel()

        return self.__channel

    def get_message(self, queue) -> Optional[FetchedMessage]:
        LOGGER.info(f"Fetching message from queue '{queue}'.")

        frame, properties, body = self._channel.basic_get(queue, auto_ack=True)

        if frame is None:
            LOGGER.info("There is no message on the queue.")
            return None

        MESSAGE_LOGGER.info(f"Fetched message with body:  {body.decode()}")

        return FetchedMessage(headers=properties.headers, body=body)
