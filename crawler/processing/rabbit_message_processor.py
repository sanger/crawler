import logging
from collections import namedtuple

from crawler.constants import (
    RABBITMQ_HEADER_KEY_SUBJECT,
    RABBITMQ_HEADER_KEY_VERSION,
    RABBITMQ_SUBJECT_CREATE_PLATE_MAP,
    RABBITMQ_SUBJECT_CREATE_PLATE_MAP_FEEDBACK,
)
from crawler.exceptions import RabbitProcessingError
from crawler.rabbit.avro_encoder import AvroEncoder

MESSAGE_SUBJECTS = (RABBITMQ_SUBJECT_CREATE_PLATE_MAP, RABBITMQ_SUBJECT_CREATE_PLATE_MAP_FEEDBACK)

LOGGER = logging.getLogger(__name__)

Headers = namedtuple("Headers", ["subject", "version"])


class RabbitMessageProcessor:
    def __init__(self, schema_registry, basic_publisher, config):
        self._encoders = {subject: AvroEncoder(schema_registry, subject) for subject in MESSAGE_SUBJECTS}
        self._basic_publisher = basic_publisher
        self._config = config

    def process_message(self, headers, body, acknowledge):
        try:
            parsed_headers = RabbitMessageProcessor._parse_headers(headers)
            LOGGER.info("Subject: %s, Version: %s", parsed_headers.subject, parsed_headers.version)
            acknowledge(True)
            self._publish_feedback_message()
        except RabbitProcessingError as ex:
            LOGGER.error("RabbitMQ message failed to process correctly: %s", ex.message)
            if ex.is_transient:
                raise  # Restart the consumer to try the message again -- possibly need to add a delay somehow.
            else:
                # Reject the message and move on.
                acknowledge(False)
                self._publish_feedback_message(ex.message)
        except Exception as ex:
            LOGGER.error("Unexpected error while processing RabbitMQ message: %s %s", type(ex), str(ex))
            self._publish_feedback_message(str(ex))

    @staticmethod
    def _parse_headers(headers):
        try:
            subject = headers[RABBITMQ_HEADER_KEY_SUBJECT]
            version = headers[RABBITMQ_HEADER_KEY_VERSION]
        except KeyError as ex:
            raise RabbitProcessingError(f"Message headers did not include required key {str(ex)}.")

        return Headers(subject, version)

    def _publish_feedback_message(self, *errors):
        pass
