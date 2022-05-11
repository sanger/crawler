import logging

from crawler.config.defaults import RABBITMQ_FEEDBACK_EXCHANGE
from crawler.constants import (
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_PARSING,
    RABBITMQ_ROUTING_KEY_CREATE_PLATE_FEEDBACK,
    RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK,
)
from crawler.exceptions import TransientRabbitError
from crawler.processing.create_plate_validator import CreatePlateValidator
from crawler.rabbit.avro_encoder import AvroEncoder
from crawler.rabbit.messages.create_feedback_message import CreateFeedbackMessage
from crawler.rabbit.messages.create_plate_message import CreatePlateError, CreatePlateMessage

LOGGER = logging.getLogger(__name__)


class CreatePlateProcessor:
    def __init__(self, schema_registry, basic_publisher, config):
        self._encoder = AvroEncoder(schema_registry, RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK)
        self._basic_publisher = basic_publisher
        self._config = config

    def process(self, message):
        self._centres = None
        create_message = CreatePlateMessage(message.message)
        validator = CreatePlateValidator(create_message, self._config)

        try:
            validator.validate()
            if len(create_message.errors) == 0:
                # TODO: Insert into MongoDB and DART
                pass
        except TransientRabbitError as ex:
            LOGGER.error(f"Transient error while processing message: {ex.message}")
            raise  # Cause the consumer to restart and try this message again.  Ideally we will delay the consumer.
        except Exception as ex:
            LOGGER.error(f"Unhandled error while processing message: {type(ex)} {str(ex)}")
            create_message.add_error(
                CreatePlateError(
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_PARSING,
                    description="An unhandled error occurred while processing the message.",
                )
            )
            self._publish_feedback(create_message)
            return False  # Send the message to dead letters

        self._publish_feedback(create_message)
        return len(create_message.feedback_errors) == 0

    def _publish_feedback(self, create_message):
        message_uuid = create_message.message_uuid.value
        feedback_message = CreateFeedbackMessage(
            sourceMessageUuid=message_uuid,
            countOfTotalSamples=create_message.total_samples,
            countOfValidSamples=create_message.validated_samples,
            operationWasErrorFree=len(create_message.feedback_errors) == 0,
            errors=create_message.feedback_errors,
        )

        encoded_message = self._encoder.encode([feedback_message])
        self._basic_publisher.publish_message(
            RABBITMQ_FEEDBACK_EXCHANGE,
            RABBITMQ_ROUTING_KEY_CREATE_PLATE_FEEDBACK,
            encoded_message.body,
            RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK,
            encoded_message.version,
        )
