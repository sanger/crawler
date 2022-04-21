import logging

from crawler.constants import RABBITMQ_SUBJECT_CREATE_PLATE
from crawler.processing.create_plate_processor import CreatePlateProcessor
from crawler.processing.rabbit_message import RabbitMessage
from crawler.rabbit.avro_encoder import AvroEncoder

LOGGER = logging.getLogger(__name__)


class RabbitMessageProcessor:
    def __init__(self, schema_registry, basic_publisher, config):
        self._schema_registry = schema_registry
        self._basic_publisher = basic_publisher
        self._config = config

        self._processors = {
            RABBITMQ_SUBJECT_CREATE_PLATE: CreatePlateProcessor(
                self._schema_registry, self._basic_publisher, self._config
            )
        }

    def process_message(self, headers, body):
        message = RabbitMessage(headers, body)
        try:
            message.decode(AvroEncoder(self._schema_registry, message.subject))
        except Exception as ex:
            LOGGER.error(f"Unrecoverable error while decoding RabbitMQ message: {type(ex)} {str(ex)}")
            return False  # Send the message to dead letters.

        if not message.contains_single_message:
            LOGGER.error("RabbitMQ message received containing multiple AVRO encoded messages.")
            return False  # Send the message to dead letters.

        try:
            return self._processors[message.subject].process(message)
        except KeyError:
            LOGGER.error(
                f"Received message has subject '{message.subject}'"
                " but there is no implemented processor for this subject."
            )
            return False  # Send the message to dead letters.