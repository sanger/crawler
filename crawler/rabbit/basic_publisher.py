import logging
import os
import ssl

from pika import BasicProperties, BlockingConnection, ConnectionParameters, PlainCredentials, SSLOptions
from pika.spec import PERSISTENT_DELIVERY_MODE

from crawler.constants import LOGGER_NAME_RABBIT_MESSAGES, RABBITMQ_HEADER_KEY_SUBJECT, RABBITMQ_HEADER_KEY_VERSION
from crawler.types import RabbitServerDetails

MESSAGE_LOGGER = logging.getLogger(LOGGER_NAME_RABBIT_MESSAGES)


class BasicPublisher:
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

    def publish_message(self, exchange, routing_key, body, subject, schema_version):
        MESSAGE_LOGGER.info(
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
        channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            properties=properties,
            body=body,
        )
        connection.close()
