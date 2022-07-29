import logging

from crawler.config.defaults import RABBITMQ_FEEDBACK_EXCHANGE
from crawler.constants import (
    CENTRE_KEY_FEEDBACK_ROUTING_KEY_PREFIX,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_PARSING,
    RABBITMQ_ROUTING_KEY_CREATE_PLATE_FEEDBACK,
    RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK,
)
from crawler.exceptions import TransientRabbitError
from crawler.processing.base_processor import BaseProcessor
from crawler.processing.create_plate_exporter import CreatePlateExporter
from crawler.processing.create_plate_validator import CreatePlateValidator
from crawler.rabbit.avro_encoder import AvroEncoder
from crawler.rabbit.messages.create_feedback_message import CreateFeedbackMessage
from crawler.rabbit.messages.parsers.create_plate_message import CreatePlateError, CreatePlateMessage, ErrorType

LOGGER = logging.getLogger(__name__)


class CreatePlateProcessor(BaseProcessor):
    def __init__(self, schema_registry, basic_publisher, config):
        self._encoder = AvroEncoder(schema_registry, RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK)
        self._basic_publisher = basic_publisher
        self._config = config

    def process(self, message):
        create_message = CreatePlateMessage(message.message)
        validator = CreatePlateValidator(create_message, self._config)
        exporter = CreatePlateExporter(create_message, self._config)

        LOGGER.info(f"Starting processing of create message with UUID '{create_message.message_uuid}'")

        # First validate the message and then export the source plate and samples to MongoDB.
        try:
            validator.validate()
            if not create_message.has_errors:
                exporter.export_to_mongo()
        except TransientRabbitError as ex:
            LOGGER.error(f"Transient error while processing message: {ex.message}")
            raise  # Cause the consumer to restart and try this message again.
        except Exception as ex:
            LOGGER.error(f"Unhandled error while processing message: {type(ex)} {str(ex)}")
            create_message.add_error(
                CreatePlateError(
                    type=ErrorType.UnhandledProcessingError,
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_PARSING,
                    description="An unhandled error occurred while processing the message.",
                )
            )

        # At this point, publish feedback as all remaining errors are not for PAM to be concerned with.
        self._publish_feedback(create_message)

        # We don't want to continue with the export to DART if we weren't able to get the samples into MongoDB.
        if create_message.has_errors:
            exporter.record_import()
            return False  # Send the message to dead letters

        # Export to DART and record the import no matter the success or not of prior steps.  Then acknowledge the
        # message as processed since PAM cannot fix issues we had with DART export or recording the import.
        exporter.export_to_dart()
        exporter.record_import()

        LOGGER.info(f"Finished processing of create message with UUID '{create_message.message_uuid}'")

        return True  # Acknowledge the message has been processed

    def _feedback_routing_key(self, centre_config):
        prefix = centre_config.get(CENTRE_KEY_FEEDBACK_ROUTING_KEY_PREFIX, "")
        return prefix + RABBITMQ_ROUTING_KEY_CREATE_PLATE_FEEDBACK

    def _publish_feedback(self, create_message):
        feedback_message = CreateFeedbackMessage(
            sourceMessageUuid=create_message.message_uuid.value,
            countOfTotalSamples=create_message.total_samples,
            countOfValidSamples=create_message.validated_samples,
            operationWasErrorFree=not create_message.has_errors,
            errors=create_message.feedback_errors,
        )

        encoded_message = self._encoder.encode([feedback_message])
        self._basic_publisher.publish_message(
            RABBITMQ_FEEDBACK_EXCHANGE,
            self._feedback_routing_key(create_message.centre_config),
            encoded_message.body,
            RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK,
            encoded_message.version,
        )
