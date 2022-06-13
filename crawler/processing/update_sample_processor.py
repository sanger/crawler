import logging

from crawler.config.defaults import RABBITMQ_FEEDBACK_EXCHANGE
from crawler.constants import (
    RABBITMQ_ROUTING_KEY_UPDATE_SAMPLE_FEEDBACK,
    RABBITMQ_SUBJECT_UPDATE_SAMPLE_FEEDBACK,
    RABBITMQ_UPDATE_FEEDBACK_ORIGIN_PARSING,
)
from crawler.exceptions import TransientRabbitError
from crawler.processing.base_processor import BaseProcessor
from crawler.processing.update_sample_validator import UpdateSampleValidator
from crawler.rabbit.avro_encoder import AvroEncoder
from crawler.rabbit.messages.update_feedback_message import UpdateFeedbackMessage
from crawler.rabbit.messages.update_sample_message import ErrorType, UpdateSampleError, UpdateSampleMessage

LOGGER = logging.getLogger(__name__)


class UpdateSampleProcessor(BaseProcessor):
    def __init__(self, schema_registry, basic_publisher, config):
        self._encoder = AvroEncoder(schema_registry, RABBITMQ_SUBJECT_UPDATE_SAMPLE_FEEDBACK)
        self._basic_publisher = basic_publisher
        self._config = config

    def process(self, message):
        update_message = UpdateSampleMessage(message.message)
        validator = UpdateSampleValidator(update_message)

        # First validate the message and then export the updates to MongoDB.
        try:
            validator.validate()
            if not update_message.has_errors:
                # Export here
                pass
        except TransientRabbitError as ex:
            LOGGER.error(f"Transient error while processing message: {ex.message}")
            raise  # Cause the consumer to restart and try this message again.  Ideally we will delay the consumer.
        except Exception as ex:
            LOGGER.error(f"Unhandled error while processing message: {type(ex)} {str(ex)}")
            update_message.add_error(
                UpdateSampleError(
                    type=ErrorType.UnhandledProcessingError,
                    origin=RABBITMQ_UPDATE_FEEDBACK_ORIGIN_PARSING,
                    description="An unhandled error occurred while processing the message.",
                )
            )

        self._publish_feedback(update_message)

        return not update_message.has_errors  # Acknowledge the message as either successful or to go to dead letters

    def _publish_feedback(self, update_message):
        feedback_message = UpdateFeedbackMessage(
            sourceMessageUuid=update_message.message_uuid.value,
            operationWasErrorFree=not update_message.has_errors,
            errors=update_message.feedback_errors,
        )

        encoded_message = self._encoder.encode([feedback_message])
        self._basic_publisher.publish_message(
            RABBITMQ_FEEDBACK_EXCHANGE,
            RABBITMQ_ROUTING_KEY_UPDATE_SAMPLE_FEEDBACK,
            encoded_message.body,
            RABBITMQ_SUBJECT_UPDATE_SAMPLE_FEEDBACK,
            encoded_message.version,
        )
