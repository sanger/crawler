import logging

from crawler.config.centres import CENTRE_DATA_SOURCE_RABBITMQ, get_centres_config
from crawler.config.defaults import RABBITMQ_FEEDBACK_EXCHANGE
from crawler.constants import (
    CENTRE_KEY_LAB_ID_DEFAULT,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_PARSING,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
    RABBITMQ_FIELD_COG_UK_ID,
    RABBITMQ_FIELD_LAB_ID,
    RABBITMQ_FIELD_MESSAGE_UUID,
    RABBITMQ_FIELD_PLATE,
    RABBITMQ_FIELD_PLATE_BARCODE,
    RABBITMQ_FIELD_PLATE_COORDINATE,
    RABBITMQ_FIELD_RNA_ID,
    RABBITMQ_FIELD_ROOT_SAMPLE_ID,
    RABBITMQ_FIELD_SAMPLE_UUID,
    RABBITMQ_FIELD_SAMPLES,
    RABBITMQ_ROUTING_KEY_CREATE_PLATE_FEEDBACK,
    RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK,
)
from crawler.exceptions import TransientRabbitError
from crawler.helpers.general_helpers import extract_duplicated_values as extract_dup_values
from crawler.rabbit.avro_encoder import AvroEncoder
from crawler.rabbit.messages.create_feedback_message import CreateFeedbackError, CreateFeedbackMessage

LOGGER = logging.getLogger(__name__)

COUNT_KEY_TOTAL_SAMPLES = "totalSamples"
COUNT_KEY_VALID_SAMPLES = "validSamples"


class CreatePlateProcessor:
    def __init__(self, schema_registry, basic_publisher, config):
        self._encoder = AvroEncoder(schema_registry, RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK)
        self._basic_publisher = basic_publisher
        self._config = config

        self._centres = None

    def process(self, message):
        self._centres = None
        message.initiate_count(COUNT_KEY_TOTAL_SAMPLES)
        message.initiate_count(COUNT_KEY_VALID_SAMPLES)

        try:
            self._validate_plate(message)
            self._validate_samples(message)
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
            countOfTotalSamples=message.get_count(COUNT_KEY_TOTAL_SAMPLES),
            countOfValidSamples=message.get_count(COUNT_KEY_VALID_SAMPLES),
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

    def _validate_plate(self, message):
        """Perform validation of the plate field in the message values for sanity.
        This does not check that the message can be inserted into the relevant databases.
        """
        plate_body = message.message[RABBITMQ_FIELD_PLATE]

        # Check that the plate is from a centre we are accepting RabbitMQ messages for.
        lab_id = plate_body[RABBITMQ_FIELD_LAB_ID]
        if lab_id not in [c[CENTRE_KEY_LAB_ID_DEFAULT] for c in self.centres]:
            CreatePlateProcessor._add_error(
                message,
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
                f"The lab ID provided '{lab_id}' is not configured to receive messages via RabbitMQ.",
                field=RABBITMQ_FIELD_LAB_ID,
            )

        # Ensure that the plate barcode isn't an empty string.
        plate_barcode = plate_body[RABBITMQ_FIELD_PLATE_BARCODE]
        if not plate_barcode:
            CreatePlateProcessor._add_error(
                message,
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
                "Field value was not populated.",
                field=RABBITMQ_FIELD_PLATE_BARCODE,
            )

    def _validate_samples(self, message):
        """Perform validation of the samples array in the message.
        This does not check that the message can be inserted into the relevant databases.
        """
        samples = message.message[RABBITMQ_FIELD_PLATE][RABBITMQ_FIELD_SAMPLES]

        # Extract all values that are supposed to be unique
        dup_values = {
            RABBITMQ_FIELD_SAMPLE_UUID: extract_dup_values([s[RABBITMQ_FIELD_SAMPLE_UUID] for s in samples]),
            RABBITMQ_FIELD_ROOT_SAMPLE_ID: extract_dup_values([s[RABBITMQ_FIELD_ROOT_SAMPLE_ID] for s in samples]),
            RABBITMQ_FIELD_PLATE_COORDINATE: extract_dup_values([s[RABBITMQ_FIELD_PLATE_COORDINATE] for s in samples]),
            RABBITMQ_FIELD_RNA_ID: extract_dup_values([s[RABBITMQ_FIELD_RNA_ID] for s in samples]),
            RABBITMQ_FIELD_COG_UK_ID: extract_dup_values(
                [s[RABBITMQ_FIELD_COG_UK_ID] for s in samples if RABBITMQ_FIELD_COG_UK_ID in s]
            ),
        }

        for uuid in dup_values[RABBITMQ_FIELD_SAMPLE_UUID]:
            string_uuid = uuid.decode()
            CreatePlateProcessor._add_error(
                message,
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                f"Sample UUID {string_uuid} was found more than once in the message.",
                sample_uuid=string_uuid,
                field=RABBITMQ_FIELD_SAMPLE_UUID,
            )

        for sample in samples:
            message.increment_count(COUNT_KEY_TOTAL_SAMPLES)
            if self._validate_sample(sample, message, dup_values):
                message.increment_count(COUNT_KEY_VALID_SAMPLES)

    def _validate_sample_field_populated(self, field, sample, message):
        if not sample[field]:
            origin = RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE
            description = "Field value was not populated."
            sample_uuid = sample[RABBITMQ_FIELD_SAMPLE_UUID].decode()
            CreatePlateProcessor._add_error(message, origin, description, sample_uuid, field)

            return False

        return True

    def _validate_sample_field_unique(self, dup_values, field, sample, message):
        if sample[field] in dup_values[field]:
            origin = RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE
            description = f"Field value provided was not unique across samples ({sample[field]})."
            sample_uuid = sample[RABBITMQ_FIELD_SAMPLE_UUID].decode()
            CreatePlateProcessor._add_error(message, origin, description, sample_uuid, field)

            return False

        return True

    def _validate_sample(self, sample, message, dup_values):
        """Perform validation of complete and consistent data for a single sample in the message."""
        valid = True

        # Ensure the sample UUID is unique
        if sample[RABBITMQ_FIELD_SAMPLE_UUID] in dup_values[RABBITMQ_FIELD_SAMPLE_UUID]:
            valid = False

        # Validate root sample ID
        if not self._validate_sample_field_populated(
            RABBITMQ_FIELD_ROOT_SAMPLE_ID, sample, message
        ) or not self._validate_sample_field_unique(dup_values, RABBITMQ_FIELD_ROOT_SAMPLE_ID, sample, message):
            valid = False

        # Validate RNA ID
        if not self._validate_sample_field_populated(
            RABBITMQ_FIELD_RNA_ID, sample, message
        ) or not self._validate_sample_field_unique(dup_values, RABBITMQ_FIELD_RNA_ID, sample, message):
            valid = False

        # Ensure sample UUIDs are all unique in this message.
        # Root sample IDs are unique and all populated with a value.
        # RNA IDs are all unique and populated.
        # Plate coordinates are unique and match the expected pattern of A-H 1-12.
        # Tested dates pre-date the message creation date.

        return valid
