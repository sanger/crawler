import logging
from typing import Any, Dict, Optional, cast

from crawler.exceptions import TransientRabbitError
from crawler.processing.base_processor import BaseProcessor
from crawler.processing.rabbit_message import RabbitMessage
from crawler.rabbit.avro_encoder import AvroEncoder

LOGGER = logging.getLogger(__name__)


class RabbitMessageProcessor:
    def __init__(self, schema_registry, basic_publisher, config):
        self._schema_registry = schema_registry
        self._basic_publisher = basic_publisher
        self._config = config

        self.__processors: Optional[Dict[str, Any]] = None

    @property
    def _processors(self) -> Dict[str, Any]:
        if self.__processors is None:
            self.__processors = {
                subject: self._build_processor_for_subject(subject) for subject in self._config.PROCESSORS.keys()
            }

        return self.__processors

    def _build_processor_for_subject(self, subject: str) -> BaseProcessor:
        processor_instance_builder = self._config.PROCESSORS[subject]
        return cast(
            BaseProcessor, processor_instance_builder(self._schema_registry, self._basic_publisher, self._config)
        )

    def process_message(self, headers, body):
        message = RabbitMessage(headers, body)
        try:
            message.decode(AvroEncoder(self._schema_registry, message.subject))
        except TransientRabbitError as ex:
            LOGGER.error(f"Transient error while processing message: {ex.message}")
            raise  # Cause the consumer to restart and try this message again.
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
