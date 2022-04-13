import logging
from typing import NamedTuple

from crawler.constants import (
    RABBITMQ_FEEDBACK_EXCHANGE_NAME,
    RABBITMQ_HEADER_KEY_SUBJECT,
    RABBITMQ_HEADER_KEY_VERSION,
    RABBITMQ_ROUTING_KEY_CREATE_PLATE_FEEDBACK,
    RABBITMQ_SUBJECT_CREATE_PLATE,
    RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK,
)
from crawler.exceptions import RabbitProcessingError
from crawler.rabbit.avro_encoder import AvroEncoder
from crawler.rabbit.messages.create_feedback_message import CreateFeedbackError, CreateFeedbackMessage

MESSAGE_SUBJECTS = (RABBITMQ_SUBJECT_CREATE_PLATE, RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK)

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
            self._publish_feedback(message_uuid="UUID not yet parsed")
            acknowledge(True)
        except RabbitProcessingError as ex:
            LOGGER.error("RabbitMQ message failed to process correctly: %s", ex.message)
            if ex.is_transient:
                raise  # Restart the consumer to try the message again -- possibly need to add a delay somehow.
            else:
                # Reject the message and move on.
                error = CreateFeedbackError(origin="parsing", description=ex.message)
                self._publish_feedback(errors=[error])
                acknowledge(False)
        except Exception as ex:
            description = f"Unexpected error while processing RabbitMQ message: {type(ex)} {str(ex)}"
            LOGGER.error(description)

            error = CreateFeedbackError(origin="parsing", description=description)
            self._publish_feedback(errors=[error])

    @staticmethod
    def _parse_headers(headers):
        try:
            subject = headers[RABBITMQ_HEADER_KEY_SUBJECT]
            version = headers[RABBITMQ_HEADER_KEY_VERSION]
        except KeyError as ex:
            raise RabbitProcessingError(f"Message headers did not include required key {str(ex)}.")

        return Headers(subject, version)

    def _publish_feedback(self, message_uuid="", errors=()):
        message = CreateFeedbackMessage(
            sourceMessageUuid=message_uuid,
            countOfTotalSamples=0,
            countOfValidSamples=0,
            operationWasErrorFree=len(errors) == 0,
            errors=errors,
        )
        encoded_message = self._encoders[RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK].encode([message])
        self._basic_publisher.publish_message(
            RABBITMQ_FEEDBACK_EXCHANGE_NAME,
            RABBITMQ_ROUTING_KEY_CREATE_PLATE_FEEDBACK,
            encoded_message.body,
            RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK,
            encoded_message.version,
        )
