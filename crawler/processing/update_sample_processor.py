import logging

from crawler.config.defaults import RABBITMQ_FEEDBACK_EXCHANGE
from crawler.constants import RABBITMQ_ROUTING_KEY_UPDATE_SAMPLE_FEEDBACK, RABBITMQ_SUBJECT_UPDATE_SAMPLE_FEEDBACK
from crawler.processing.base_processor import BaseProcessor
from crawler.rabbit.avro_encoder import AvroEncoder
from crawler.rabbit.messages.update_feedback_message import UpdateFeedbackMessage
from crawler.rabbit.messages.update_sample_message import UpdateSampleMessage

LOGGER = logging.getLogger(__name__)


class UpdateSampleProcessor(BaseProcessor):
    def __init__(self, schema_registry, basic_publisher, config):
        self._encoder = AvroEncoder(schema_registry, RABBITMQ_SUBJECT_UPDATE_SAMPLE_FEEDBACK)
        self._basic_publisher = basic_publisher
        self._config = config

    def process(self, message):
        update_message = UpdateSampleMessage(message.message)
        self._publish_feedback(update_message)

        return True  # Acknowledge the message has been processed

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
