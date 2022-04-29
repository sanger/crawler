import logging
import re

from crawler.config.centres import CENTRE_DATA_SOURCE_RABBITMQ, get_centres_config
from crawler.constants import (
    CENTRE_KEY_LAB_ID_DEFAULT,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
    RABBITMQ_FIELD_COG_UK_ID,
    RABBITMQ_FIELD_LAB_ID,
    RABBITMQ_FIELD_MESSAGE_CREATE_DATE,
    RABBITMQ_FIELD_PLATE,
    RABBITMQ_FIELD_PLATE_BARCODE,
    RABBITMQ_FIELD_PLATE_COORDINATE,
    RABBITMQ_FIELD_RNA_ID,
    RABBITMQ_FIELD_ROOT_SAMPLE_ID,
    RABBITMQ_FIELD_SAMPLE_UUID,
    RABBITMQ_FIELD_SAMPLES,
    RABBITMQ_FIELD_TESTED_DATE,
)
from crawler.exceptions import TransientRabbitError
from crawler.helpers.general_helpers import extract_duplicated_values as extract_dupes
from crawler.rabbit.messages.create_feedback_message import CreateFeedbackError

LOGGER = logging.getLogger(__name__)


class CreatePlateValidator:
    def __init__(self, message, config):
        self._config = config
        self.message = message
        self.total_samples = 0
        self.valid_samples = 0

        self._centres = None
        self._errors = []

    def validate(self):
        self.total_samples = 0
        self.valid_samples = 0

        self._validate_plate()
        self._validate_samples()

    def _add_error(self, origin, description, sample_uuid="", field=""):
        LOGGER.error(
            f"Error found in message with origin '{origin}', sampleUuid '{sample_uuid}', field '{field}': {description}"
        )
        self._errors.append(
            CreateFeedbackError(
                origin=origin,
                sampleUuid=sample_uuid,
                field=field,
                description=description,
            )
        )

    @property
    def centres(self):
        if self._centres is None:
            try:
                self._centres = get_centres_config(self._config, CENTRE_DATA_SOURCE_RABBITMQ)
            except Exception:
                raise TransientRabbitError("Unable to reach MongoDB while getting centres config.")

        return self._centres

    @property
    def errors(self):
        return self._errors.copy()

    def _validate_plate(self):
        """Perform validation of the plate field in the message values for sanity.
        This does not check that the message can be inserted into the relevant databases.
        """
        plate_body = self.message[RABBITMQ_FIELD_PLATE]

        # Check that the plate is from a centre we are accepting RabbitMQ messages for.
        lab_id = plate_body[RABBITMQ_FIELD_LAB_ID]
        if lab_id not in [c[CENTRE_KEY_LAB_ID_DEFAULT] for c in self.centres]:
            self._add_error(
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
                f"The lab ID provided '{lab_id}' is not configured to receive messages via RabbitMQ.",
                field=RABBITMQ_FIELD_LAB_ID,
            )

        # Ensure that the plate barcode isn't an empty string.
        plate_barcode = plate_body[RABBITMQ_FIELD_PLATE_BARCODE]
        if not plate_barcode:
            self._add_error(
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
                "Field value was not populated.",
                field=RABBITMQ_FIELD_PLATE_BARCODE,
            )

    def _validate_samples(self):
        """Perform validation of the samples array in the message.
        This does not check that the message can be inserted into the relevant databases.
        """
        samples = self.message[RABBITMQ_FIELD_PLATE][RABBITMQ_FIELD_SAMPLES]

        # Extract all values that are supposed to be unique
        dup_values = {
            RABBITMQ_FIELD_SAMPLE_UUID: extract_dupes([s[RABBITMQ_FIELD_SAMPLE_UUID] for s in samples]),
            RABBITMQ_FIELD_ROOT_SAMPLE_ID: extract_dupes([s[RABBITMQ_FIELD_ROOT_SAMPLE_ID] for s in samples]),
            RABBITMQ_FIELD_PLATE_COORDINATE: extract_dupes([s[RABBITMQ_FIELD_PLATE_COORDINATE] for s in samples]),
            RABBITMQ_FIELD_RNA_ID: extract_dupes([s[RABBITMQ_FIELD_RNA_ID] for s in samples]),
            RABBITMQ_FIELD_COG_UK_ID: extract_dupes(
                [s[RABBITMQ_FIELD_COG_UK_ID] for s in samples if RABBITMQ_FIELD_COG_UK_ID in s]
            ),
        }

        for uuid in dup_values[RABBITMQ_FIELD_SAMPLE_UUID]:
            string_uuid = uuid.decode()
            self._add_error(
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                f"Sample UUID {string_uuid} exists more than once in the message.",
                sample_uuid=string_uuid,
                field=RABBITMQ_FIELD_SAMPLE_UUID,
            )

        for sample in samples:
            self.total_samples += 1
            if self._validate_sample(sample, dup_values):
                self.valid_samples += 1

    def _validate_sample_field_populated(self, field, sample):
        if not sample[field]:
            origin = RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE
            description = "Field value is not populated."
            sample_uuid = sample[RABBITMQ_FIELD_SAMPLE_UUID].decode()
            self._add_error(origin, description, sample_uuid, field)

            return False

        return True

    def _validate_sample_field_unique(self, dup_values, field, sample):
        if sample[field] in dup_values[field]:
            origin = RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE
            description = f"Field value is not unique across samples ({sample[field]})."
            sample_uuid = sample[RABBITMQ_FIELD_SAMPLE_UUID].decode()
            self._add_error(origin, description, sample_uuid, field)

            return False

        return True

    def _validate_sample_field_matches_regex(self, regex, field, sample):
        if not regex.match(sample[field]):
            origin = RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE
            description = f"Field value does not match regex ({regex.pattern})."
            sample_uuid = sample[RABBITMQ_FIELD_SAMPLE_UUID].decode()
            self._add_error(origin, description, sample_uuid, field)

            return False

        return True

    def _validate_sample_field_no_later_than(self, timestamp, field, sample):
        if sample[field] > timestamp:
            origin = RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE
            description = f"Field value repesents a timestamp that is too recent ({sample[field]} > {timestamp})."
            sample_uuid = sample[RABBITMQ_FIELD_SAMPLE_UUID].decode()
            self._add_error(origin, description, sample_uuid, field)

            return False

        return True

    def _validate_sample(self, sample, dup_values):
        """Perform validation of complete and consistent data for a single sample in the message."""
        valid = True

        # Ensure the sample UUID is unique
        if sample[RABBITMQ_FIELD_SAMPLE_UUID] in dup_values[RABBITMQ_FIELD_SAMPLE_UUID]:
            valid = False

        # Validate root sample ID
        if not self._validate_sample_field_populated(
            RABBITMQ_FIELD_ROOT_SAMPLE_ID, sample
        ) or not self._validate_sample_field_unique(dup_values, RABBITMQ_FIELD_ROOT_SAMPLE_ID, sample):
            valid = False

        # Validate RNA ID
        if not self._validate_sample_field_populated(
            RABBITMQ_FIELD_RNA_ID, sample
        ) or not self._validate_sample_field_unique(dup_values, RABBITMQ_FIELD_RNA_ID, sample):
            valid = False

        # Validate plate coordinates
        if not self._validate_sample_field_matches_regex(
            re.compile(r"^[A-H](?:0?[1-9]|1[0-2])$"),
            RABBITMQ_FIELD_PLATE_COORDINATE,
            sample,  # A1 - H12 or A01 padded format
        ) or not self._validate_sample_field_unique(dup_values, RABBITMQ_FIELD_PLATE_COORDINATE, sample):
            valid = False

        # Validate tested date is not newer than the message create date
        message_create_date = self.message[RABBITMQ_FIELD_MESSAGE_CREATE_DATE]
        if not self._validate_sample_field_no_later_than(message_create_date, RABBITMQ_FIELD_TESTED_DATE, sample):
            valid = False

        return valid
