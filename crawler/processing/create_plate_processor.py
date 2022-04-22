import logging

from crawler.config.centres import CENTRE_DATA_SOURCE_RABBITMQ, get_centres_config
from crawler.config.defaults import RABBITMQ_FEEDBACK_EXCHANGE
from crawler.constants import (
    CENTRE_KEY_LAB_ID_DEFAULT,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_PARSING,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
    RABBITMQ_FIELD_LAB_ID,
    RABBITMQ_FIELD_MESSAGE_UUID,
    RABBITMQ_FIELD_PLATE,
    RABBITMQ_ROUTING_KEY_CREATE_PLATE_FEEDBACK,
    RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK,
)
from crawler.exceptions import TransientRabbitError
from crawler.rabbit.avro_encoder import AvroEncoder
from crawler.rabbit.messages.create_feedback_message import CreateFeedbackError, CreateFeedbackMessage

LOGGER = logging.getLogger(__name__)


class CreatePlateProcessor:
    def __init__(self, schema_registry, basic_publisher, config):
        self._encoder = AvroEncoder(schema_registry, RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK)
        self._basic_publisher = basic_publisher
        self._config = config

        self._centres = None

    def process(self, message):
        self._centres = None

        try:
            self._validate_message(message)
        except TransientRabbitError as ex:
            LOGGER.error(f"Transient error while processing message: {ex.message}")
            raise  # Cause the consumer to restart and try this message again.  Ideally we will delay the consumer.
        except Exception as ex:
            LOGGER.error(f"Unhandled error while processing message: {type(ex)} {str(ex)}")
            self._publish_feedback(
                message,
                additional_errors=[
                    CreateFeedbackError(
                        origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_PARSING,
                        description="An unhandled error occurred while processing the message.",
                    )
                ],
            )
            return False  # Send the message to dead letters

        self._publish_feedback(message)
        return len(message.errors) == 0

    @property
    def centres(self):
        if self._centres is None:
            try:
                self._centres = get_centres_config(self._config, CENTRE_DATA_SOURCE_RABBITMQ)
            except Exception:
                raise TransientRabbitError("Unable to reach MongoDB while getting centres config.")

        return self._centres

    def _publish_feedback(self, message, additional_errors=()):
        message_uuid = message.message[RABBITMQ_FIELD_MESSAGE_UUID].decode()
        errors = message.errors + list(additional_errors)

        feedback_message = CreateFeedbackMessage(
            sourceMessageUuid=message_uuid,
            countOfTotalSamples=0,
            countOfValidSamples=0,
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

    @staticmethod
    def _add_error(message, origin, description, sample_uuid="", field=""):
        LOGGER.error(
            f"Error found in message with origin '{origin}', sampleUuid '{sample_uuid}', field '{field}': {description}"
        )
        message.add_error(
            CreateFeedbackError(
                origin=origin,
                sampleUuid=sample_uuid,
                field=field,
                description=description,
            )
        )

    def _validate_message(self, message):
        body = message.message

        # Check that the message is for a centre we are accepting RabbitMQ messages for.
        lab_id = body[RABBITMQ_FIELD_PLATE][RABBITMQ_FIELD_LAB_ID]
        if lab_id not in [c[CENTRE_KEY_LAB_ID_DEFAULT] for c in self.centres]:
            CreatePlateProcessor._add_error(
                message,
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
                f"The lab ID provided '{lab_id}' is not configured to receive messages via RabbitMQ.",
                field=RABBITMQ_FIELD_LAB_ID,
            )
