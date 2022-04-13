import logging
from typing import NamedTuple

from crawler.constants import (
    RABBITMQ_HEADER_KEY_SUBJECT,
    RABBITMQ_HEADER_KEY_VERSION,
    RABBITMQ_SUBJECT_CREATE_PLATE_MAP,
    RABBITMQ_SUBJECT_CREATE_PLATE_MAP_FEEDBACK,
)
from crawler.exceptions import RabbitProcessingError
from crawler.rabbit.avro_encoder import AvroEncoder
from crawler.rabbit.messages.create_feedback_message import CreateFeedbackMessage

MESSAGE_SUBJECTS = (RABBITMQ_SUBJECT_CREATE_PLATE_MAP, RABBITMQ_SUBJECT_CREATE_PLATE_MAP_FEEDBACK)

LOGGER = logging.getLogger(__name__)


class Headers(NamedTuple):
    subject: str
    version: str


class RabbitMessageProcessor:
    def __init__(self, schema_registry, basic_publisher, config):
        self._encoders = {subject: AvroEncoder(schema_registry, subject) for subject in MESSAGE_SUBJECTS}
        self._basic_publisher = basic_publisher
        self._config = config

    def process_message(self, headers, body, acknowledge):
        try:
            _ = RabbitMessageProcessor._parse_headers(headers)
            self._publish_success("UUID not yet parsed")
            acknowledge(True)
        except RabbitProcessingError as ex:
            LOGGER.error("RabbitMQ message failed to process correctly: %s", ex.message)
            if ex.is_transient:
                raise  # Restart the consumer to try the message again -- possibly need to add a delay somehow.
            else:
                # Reject the message and move on.
                self._publish_errors(ex.message)
                acknowledge(False)
        except Exception as ex:
            LOGGER.error("Unexpected error while processing RabbitMQ message: %s %s", type(ex), str(ex))
            self._publish_errors(str(ex))

    @staticmethod
    def _parse_headers(headers):
        try:
            subject = headers[RABBITMQ_HEADER_KEY_SUBJECT]
            version = headers[RABBITMQ_HEADER_KEY_VERSION]
        except KeyError as ex:
            raise RabbitProcessingError(f"Message headers did not include required key {str(ex)}.")

        return Headers(subject, version)

    def _publish_success(self, message_uuid):
        message = CreateFeedbackMessage(
            sourceMessageUuid=message_uuid,
            countOfTotalSamples=0,
            countOfValidSamples=0,
            operationWasErrorFree=True,
            errors=[],
        )
        encoded_message = self._encoders[RABBITMQ_SUBJECT_CREATE_PLATE_MAP_FEEDBACK].encode([message])
        self._basic_publisher.publish_message(
            "psd.heron",
            "feedback.created.plate",
            encoded_message.body,
            RABBITMQ_SUBJECT_CREATE_PLATE_MAP_FEEDBACK,
            encoded_message.version,
        )

    def _publish_errors(self, message_uuid="Unknown", *errors):
        pass
