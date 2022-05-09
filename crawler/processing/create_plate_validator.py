import re

from crawler.config.centres import CENTRE_DATA_SOURCE_RABBITMQ, get_centres_config
from crawler.constants import (
    CENTRE_KEY_LAB_ID_DEFAULT,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
)
from crawler.exceptions import TransientRabbitError
from crawler.helpers.sample_data_helpers import normalise_plate_coordinate


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
        # Check that the plate is from a centre we are accepting RabbitMQ messages for.
        lab_id_field, lab_id = self._message.plate_lab_id
        if lab_id not in [c[CENTRE_KEY_LAB_ID_DEFAULT] for c in self.centres]:
            self._message.add_error(
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
                f"The lab ID provided '{lab_id}' is not configured to receive messages via RabbitMQ.",
                field=lab_id_field,
            )

        # Ensure that the plate barcode isn't an empty string.
        plate_barcode_field, plate_barcode = self._message.plate_barcode
        if not plate_barcode:
            self._message.add_error(
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
                "Field value is not populated.",
                field=plate_barcode_field,
            )

    def _validate_samples(self):
        """Perform validation of the samples array in the message.
        This does not check that the message can be inserted into the relevant databases.
        """
        if len(self._message.samples) == 0:
            return

        sample_uuid_field, _ = self._message.samples[0].sample_uuid
        for sample_uuid in self._message.duplicated_sample_values[sample_uuid_field]:
            self._message.add_error(
                RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                f"Sample UUID {sample_uuid} exists more than once in the message.",
                sample_uuid=sample_uuid,
                field=sample_uuid_field,
            )

        self._message.validated_samples = 0
        for sample in self._message.samples:
            if self._validate_sample(sample):
                self._message.validated_samples += 1

    def _validate_sample_field_populated(self, field_name, field_value, sample_uuid):
        if not field_value:
            origin = RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE
            description = "Field value is not populated."
            self._message.add_error(origin, description, sample_uuid, field_name)

            return False

        return True

    def _validate_sample_field_unique(self, field_name, field_value, sample_uuid, normalise_func=None):
        normalised_value = field_value
        if normalise_func is not None:
            normalised_value = normalise_func(normalised_value)

        if normalised_value in self._message.duplicated_sample_values[field_name]:
            origin = RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE
            description = f"Field value is not unique across samples ({field_value})."
            self._message.add_error(origin, description, sample_uuid, field_name)

            return False

        return True

    def _validate_sample_field_matches_regex(self, regex, field_name, field_value, sample_uuid):
        if not regex.match(field_value):
            origin = RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE
            description = f"Field value does not match regex ({regex.pattern})."
            self._message.add_error(origin, description, sample_uuid, field_name)

            return False

        return True

    def _validate_sample_field_no_later_than(self, timestamp, field_name, field_value, sample_uuid):
        if field_value > timestamp:
            origin = RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE
            description = f"Field value repesents a timestamp that is too recent ({field_value} > {timestamp})."
            self._message.add_error(origin, description, sample_uuid, field_name)

            return False

        return True

    def _validate_sample(self, sample):
        """Perform validation of complete and consistent data for a single sample in the message."""
        valid = True

        # Ensure the sample UUID is unique
        sample_uuid_field, sample_uuid = sample.sample_uuid
        if sample_uuid in self._message.duplicated_sample_values[sample_uuid_field]:
            valid = False

        # Validate root sample ID
        root_sample_id_field, root_sample_id = sample.root_sample_id
        if not self._validate_sample_field_populated(
            root_sample_id_field, root_sample_id, sample_uuid
        ) or not self._validate_sample_field_unique(root_sample_id_field, root_sample_id, sample_uuid):
            valid = False

        # Validate RNA ID
        rna_id_field, rna_id = sample.rna_id
        if not self._validate_sample_field_populated(
            rna_id_field, rna_id, sample_uuid
        ) or not self._validate_sample_field_unique(rna_id_field, rna_id, sample_uuid):
            valid = False

        # Validate COG UK ID
        cog_uk_id_field, cog_uk_id = sample.cog_uk_id
        if not self._validate_sample_field_populated(
            cog_uk_id_field, cog_uk_id, sample_uuid
        ) or not self._validate_sample_field_unique(cog_uk_id_field, cog_uk_id, sample_uuid):
            valid = False

        # Validate plate coordinates
        plate_coordinate_field, plate_coordinate = sample.plate_coordinate
        if not self._validate_sample_field_matches_regex(
            re.compile(r"^[A-H](?:0?[1-9]|1[0-2])$"),  # A1 - H12 or A01 padded format
            plate_coordinate_field,
            plate_coordinate,
            sample_uuid,
        ) or not self._validate_sample_field_unique(
            plate_coordinate_field, plate_coordinate, sample_uuid, normalise_plate_coordinate
        ):
            valid = False

        # Validate tested date is not newer than the message create date
        _, message_create_date = self._message.message_create_date
        sample_tested_date_field, sample_tested_date = sample.tested_date
        if not self._validate_sample_field_no_later_than(
            message_create_date, sample_tested_date_field, sample_tested_date, sample_uuid
        ):
            valid = False

        return valid
