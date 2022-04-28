import logging

from crawler.config.defaults import RABBITMQ_FEEDBACK_EXCHANGE
from crawler.constants import (
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_PARSING,
    RABBITMQ_FIELD_MESSAGE_UUID,
    RABBITMQ_ROUTING_KEY_CREATE_PLATE_FEEDBACK,
    RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK,
)
from crawler.exceptions import TransientRabbitError
from crawler.processing.create_plate_validator import CreatePlateValidator
from crawler.rabbit.avro_encoder import AvroEncoder
from crawler.rabbit.messages.create_feedback_message import CreateFeedbackError, CreateFeedbackMessage

LOGGER = logging.getLogger(__name__)


class CreatePlateProcessor:
    def __init__(self, schema_registry, basic_publisher, config):
        self._encoder = AvroEncoder(schema_registry, RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK)
        self._basic_publisher = basic_publisher
        self._config = config

    def process(self, message):
        self._centres = None
        validator = CreatePlateValidator(message.message, self._config)

        try:
            validator.validate()
        except TransientRabbitError as ex:
            LOGGER.error(f"Transient error while processing message: {ex.message}")
            raise  # Cause the consumer to restart and try this message again.  Ideally we will delay the consumer.
        except Exception as ex:
            LOGGER.error(f"Unhandled error while processing message: {type(ex)} {str(ex)}")
            self._publish_feedback(
                validator,
                additional_errors=[
                    CreateFeedbackError(
                        origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_PARSING,
                        description="An unhandled error occurred while processing the message.",
                    )
                ],
            )
            return False  # Send the message to dead letters

        self._publish_feedback(validator)
        return len(validator.errors) == 0

    def _publish_feedback(self, validator, additional_errors=()):
        message_uuid = validator.message[RABBITMQ_FIELD_MESSAGE_UUID].decode()
        errors = validator.errors + list(additional_errors)

        feedback_message = CreateFeedbackMessage(
            sourceMessageUuid=message_uuid,
            countOfTotalSamples=validator.total_samples,
            countOfValidSamples=validator.valid_samples,
            operationWasErrorFree=len(errors) == 0,
            errors=errors,
        )

        encoded_message = self._encoder.encode([feedback_message])
        self._basic_publisher.publish_message(
            RABBITMQ_FEEDBACK_EXCHANGE,
            RABBITMQ_ROUTING_KEY_CREATE_PLATE_FEEDBACK,
            encoded_message.body,
            RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK,
            encoded_message.version,
        )
