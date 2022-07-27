import logging

from crawler.config.defaults import RABBITMQ_FEEDBACK_EXCHANGE
from crawler.constants import (
    RABBITMQ_ROUTING_KEY_UPDATE_SAMPLE_FEEDBACK,
    RABBITMQ_SUBJECT_UPDATE_SAMPLE_FEEDBACK,
    RABBITMQ_UPDATE_FEEDBACK_ORIGIN_PARSING,
)
from crawler.exceptions import TransientRabbitError
from crawler.processing.base_processor import BaseProcessor
from crawler.processing.update_sample_exporter import UpdateSampleExporter
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
        exporter = UpdateSampleExporter(update_message, self._config)

        LOGGER.info(f"Starting processing of update message with UUID '{update_message.message_uuid}'")

        # First validate the message and then export the updates to MongoDB.
        try:
            validator.validate()
            if not update_message.has_errors:
                exporter.verify_sample_in_mongo()
            if not update_message.has_errors:
                exporter.verify_plate_state()
            if not update_message.has_errors:
                exporter.update_mongo()
        except TransientRabbitError as ex:
            LOGGER.error(f"Transient error while processing message: {ex.message}")
            raise  # Cause the consumer to restart and try this message again.
        except Exception as ex:
            LOGGER.error(f"Unhandled error while processing message: {type(ex)} {str(ex)}")
            update_message.add_error(
                UpdateSampleError(
                    type=ErrorType.UnhandledProcessingError,
                    origin=RABBITMQ_UPDATE_FEEDBACK_ORIGIN_PARSING,
                    description="An unhandled error occurred while processing the message.",
                )
            )

        # At this point, publish feedback as all remaining errors are not for PAM to be concerned with.
        self._publish_feedback(update_message)

        if update_message.has_errors:
            return False  # Errors up to this point mean we should send the message to dead-letters.

        exporter.update_dart()

        LOGGER.info(f"Finished processing of update message with UUID '{update_message.message_uuid}'")

        return True  # The message has been processed whether DART worked or not.

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
