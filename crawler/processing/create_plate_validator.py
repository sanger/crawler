import re

from crawler.config.centres import CENTRE_DATA_SOURCE_RABBITMQ, get_centres_config
from crawler.constants import (
    CENTRE_KEY_LAB_ID_DEFAULT,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
)
from crawler.exceptions import TransientRabbitError
from crawler.helpers.general_helpers import extract_duplicated_values as extract_dupes
from crawler.helpers.sample_data_helpers import normalise_plate_coordinate
from crawler.processing.messages.create_plate_message import (
    FIELD_COG_UK_ID,
    FIELD_LAB_ID,
    FIELD_MESSAGE_CREATE_DATE,
    FIELD_PLATE,
    FIELD_PLATE_BARCODE,
    FIELD_PLATE_COORDINATE,
    FIELD_RNA_ID,
    FIELD_ROOT_SAMPLE_ID,
    FIELD_SAMPLE_UUID,
    FIELD_SAMPLES,
    FIELD_TESTED_DATE,
)


class CreatePlateValidator:
    def __init__(self, message, config):
        self._config = config
        self._message = message

        self._centres = None

    def validate(self):
        self._validate_plate()
        self._validate_samples()

    @property
    def centres(self):
        if self._centres is None:
            try:
                self._centres = get_centres_config(self._config, CENTRE_DATA_SOURCE_RABBITMQ)
            except Exception:
                raise TransientRabbitError("Unable to reach MongoDB while getting centres config.")

        return self._centres

    def _validate_plate(self):
        """Perform validation of the plate field in the message values for sanity.
        This does not check that the message can be inserted into the relevant databases.
        """
        plate_body = self._message._body[FIELD_PLATE]

        # Check that the plate is from a centre we are accepting RabbitMQ messages for.
        lab_id = plate_body[FIELD_LAB_ID]
        if lab_id not in [c[CENTRE_KEY_LAB_ID_DEFAULT] for c in self.centres]:
            self._message.add_error(
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
                f"The lab ID provided '{lab_id}' is not configured to receive messages via RabbitMQ.",
                field=FIELD_LAB_ID,
            )

        # Ensure that the plate barcode isn't an empty string.
        plate_barcode = plate_body[FIELD_PLATE_BARCODE]
        if not plate_barcode:
            self._message.add_error(
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
                "Field value is not populated.",
                field=FIELD_PLATE_BARCODE,
            )

    def _validate_samples(self):
        """Perform validation of the samples array in the message.
        This does not check that the message can be inserted into the relevant databases.
        """
        samples = self._message._body[FIELD_PLATE][FIELD_SAMPLES]

        # Extract all values that are supposed to be unique
        dup_values = {
            FIELD_SAMPLE_UUID: extract_dupes([s[FIELD_SAMPLE_UUID] for s in samples]),
            FIELD_ROOT_SAMPLE_ID: extract_dupes([s[FIELD_ROOT_SAMPLE_ID] for s in samples]),
            FIELD_RNA_ID: extract_dupes([s[FIELD_RNA_ID] for s in samples]),
            FIELD_COG_UK_ID: extract_dupes([s[FIELD_COG_UK_ID] for s in samples]),
            FIELD_PLATE_COORDINATE: extract_dupes(
                [normalise_plate_coordinate(s[FIELD_PLATE_COORDINATE]) for s in samples]
            ),
        }

        for uuid in dup_values[FIELD_SAMPLE_UUID]:
            string_uuid = uuid.decode()
            self._message.add_error(
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                f"Sample UUID {string_uuid} exists more than once in the message.",
                sample_uuid=string_uuid,
                field=FIELD_SAMPLE_UUID,
            )

        self._message.validated_samples = 0
        for sample in samples:
            if self._validate_sample(sample, dup_values):
                self._message.validated_samples += 1

    def _validate_sample_field_populated(self, field, sample):
        if not sample[field]:
            origin = RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE
            description = "Field value is not populated."
            sample_uuid = sample[FIELD_SAMPLE_UUID].decode()
            self._message.add_error(origin, description, sample_uuid, field)

            return False

        return True

    def _validate_sample_field_unique(self, dup_values, field, sample, normalise_func=None):
        normalised_value = sample[field]
        if normalise_func is not None:
            normalised_value = normalise_func(normalised_value)

        if normalised_value in dup_values[field]:
            origin = RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE
            description = f"Field value is not unique across samples ({sample[field]})."
            sample_uuid = sample[FIELD_SAMPLE_UUID].decode()
            self._message.add_error(origin, description, sample_uuid, field)

            return False

        return True

    def _validate_sample_field_matches_regex(self, regex, field, sample):
        if not regex.match(sample[field]):
            origin = RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE
            description = f"Field value does not match regex ({regex.pattern})."
            sample_uuid = sample[FIELD_SAMPLE_UUID].decode()
            self._message.add_error(origin, description, sample_uuid, field)

            return False

        return True

    def _validate_sample_field_no_later_than(self, timestamp, field, sample):
        if sample[field] > timestamp:
            origin = RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE
            description = f"Field value repesents a timestamp that is too recent ({sample[field]} > {timestamp})."
            sample_uuid = sample[FIELD_SAMPLE_UUID].decode()
            self._message.add_error(origin, description, sample_uuid, field)

            return False

        return True

    def _validate_sample(self, sample, dup_values):
        """Perform validation of complete and consistent data for a single sample in the message."""
        valid = True

        # Ensure the sample UUID is unique
        if sample[FIELD_SAMPLE_UUID] in dup_values[FIELD_SAMPLE_UUID]:
            valid = False

        # Validate root sample ID
        if not self._validate_sample_field_populated(
            FIELD_ROOT_SAMPLE_ID, sample
        ) or not self._validate_sample_field_unique(dup_values, FIELD_ROOT_SAMPLE_ID, sample):
            valid = False

        # Validate RNA ID
        if not self._validate_sample_field_populated(FIELD_RNA_ID, sample) or not self._validate_sample_field_unique(
            dup_values, FIELD_RNA_ID, sample
        ):
            valid = False

        # Validate COG UK ID
        if not self._validate_sample_field_populated(FIELD_COG_UK_ID, sample) or not self._validate_sample_field_unique(
            dup_values, FIELD_COG_UK_ID, sample
        ):
            valid = False

        # Validate plate coordinates
        if not self._validate_sample_field_matches_regex(
            re.compile(r"^[A-H](?:0?[1-9]|1[0-2])$"),  # A1 - H12 or A01 padded format
            FIELD_PLATE_COORDINATE,
            sample,
        ) or not self._validate_sample_field_unique(
            dup_values, FIELD_PLATE_COORDINATE, sample, normalise_plate_coordinate
        ):
            valid = False

        # Validate tested date is not newer than the message create date
        message_create_date = self._message._body[FIELD_MESSAGE_CREATE_DATE]
        if not self._validate_sample_field_no_later_than(message_create_date, FIELD_TESTED_DATE, sample):
            valid = False

        return valid
