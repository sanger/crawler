import logging

from crawler.config.defaults import RABBITMQ_FEEDBACK_EXCHANGE
from crawler.constants import (
    RABBITMQ_ROUTING_KEY_CREATE_PLATE_FEEDBACK,
    RABBITMQ_SUBJECT_CREATE_PLATE,
    RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK,
)
from crawler.exceptions import RabbitProcessingError
from crawler.processing.rabbit_message import RabbitMessage
from crawler.rabbit.avro_encoder import AvroEncoder
from crawler.rabbit.messages.create_feedback_message import CreateFeedbackMessage

MESSAGE_SUBJECTS = (RABBITMQ_SUBJECT_CREATE_PLATE, RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK)

LOGGER = logging.getLogger(__name__)


class RabbitMessageProcessor:
    def __init__(self, schema_registry, basic_publisher, config):
        self._encoders = {subject: AvroEncoder(schema_registry, subject) for subject in MESSAGE_SUBJECTS}
        self._basic_publisher = basic_publisher
        self._config = config

    def process_message(self, headers, body):
        message = RabbitMessage(headers, body)
        try:
            message.decode(self._encoders[message.subject])
        except Exception as ex:
            LOGGER.error(f"Unrecoverable error while decoding RabbitMQ message: {type(ex)} {str(ex)}")
            return False  # Send the message to dead letters.

        if not message.contains_single_message:
            return False  # Send the message to dead letters.

        try:
            # At this point we can definitely read our message and start publishing feedback.
            LOGGER.debug(message.message)
        except RabbitProcessingError as ex:
            LOGGER.error(f"Error while processing message: {ex.message}")
            # TODO: Publish feedback about errors recorded in the message object.
            if ex.is_transient:
                raise  # Cause the consumer to restart and try this message again.  Ideally we will delay the consumer.
            else:
                return False  # Send the message to dead letters.
        except Exception as ex:
            LOGGER.error(f"Unexpected error type while processing RabbitMQ message: {type(ex)} {str(ex)}")
            # TODO: Publish feedback about the unexpected error condition.
            return False  # Send the message to dead letters

        return True  # For now acknowledge anything we successfully decode

    def _publish_feedback(self, message_uuid, errors=()):
        message = CreateFeedbackMessage(
            sourceMessageUuid=message_uuid,
            countOfTotalSamples=0,
            countOfValidSamples=0,
            operationWasErrorFree=len(errors) == 0,
            errors=errors,
        )
        encoded_message = self._encoders[RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK].encode([message])
        self._basic_publisher.publish_message(
            RABBITMQ_FEEDBACK_EXCHANGE,
            RABBITMQ_ROUTING_KEY_CREATE_PLATE_FEEDBACK,
            encoded_message.body,
            RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK,
            encoded_message.version,
        )
