import logging
import os
import ssl
import time

from pika import BasicProperties, BlockingConnection, ConnectionParameters, PlainCredentials, SSLOptions
from pika.exceptions import UnroutableError
from pika.spec import PERSISTENT_DELIVERY_MODE

from crawler.constants import LOGGER_NAME_RABBIT_MESSAGES, RABBITMQ_HEADER_KEY_SUBJECT, RABBITMQ_HEADER_KEY_VERSION
from crawler.types import RabbitServerDetails

LOGGER = logging.getLogger(__name__)
MESSAGE_LOGGER = logging.getLogger(LOGGER_NAME_RABBIT_MESSAGES)


class BasicPublisher:
    def __init__(self, server_details: RabbitServerDetails, publish_retry_delay: int, publish_max_retries: int):
        self._publish_retry_delay = publish_retry_delay
        self._publish_max_retries = publish_max_retries
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

    def publish_message(self, exchange, routing_key, body, subject, schema_version):
        LOGGER.info(
            f"Publishing message to exchange '{exchange}', routing key '{routing_key}', "
            f"schema subject '{subject}', schema version '{schema_version}'."
        )
        MESSAGE_LOGGER.info(f"Published message body:  {body.decode()}")
        properties = BasicProperties(
            delivery_mode=PERSISTENT_DELIVERY_MODE,
            headers={
                RABBITMQ_HEADER_KEY_SUBJECT: subject,
                RABBITMQ_HEADER_KEY_VERSION: schema_version,
            },
        )

        connection = BlockingConnection(self._connection_params)
        channel = connection.channel()
        channel.confirm_delivery()  # Force exceptions when Rabbit cannot deliver the message
        self._do_publish_with_retry(
            lambda: channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body, properties=properties)
        )
        connection.close()

    def _do_publish_with_retry(self, publish_method):
        retry_count = 0

        while True:
            try:
                publish_method()
                break  # When no exception is thrown from publish_method we'll break out of the while loop
            except UnroutableError:
                retry_count += 1

                if retry_count == self._publish_max_retries:
                    LOGGER.error(
                        "Maximum number of retries exceeded for message being published to RabbitMQ. "
                        "Message was NOT PUBLISHED!"
                    )
                    return

                time.sleep(self._publish_retry_delay)

        if retry_count > 0:
            LOGGER.error(f"Publish of message to RabbitMQ required {retry_count} retries.")

        LOGGER.info("The message was published to RabbitMQ successfully.")
