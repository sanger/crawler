import re

from crawler.config.centres import CENTRE_DATA_SOURCE_RABBITMQ, get_centres_config
from crawler.constants import (
    CENTRE_KEY_LAB_ID_DEFAULT,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
)
from crawler.exceptions import TransientRabbitError
from crawler.helpers.sample_data_helpers import normalise_plate_coordinate
from crawler.rabbit.messages.create_plate_message import CreatePlateError, ErrorType


class CreatePlateValidator:
    def __init__(self, message, config):
        self._message = message
        self._config = config

        self._centres = None

    def validate(self):
        self._set_centre_conf()
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

    def _set_centre_conf(self):
        """Find a centre from the list of those we're accepting RabbitMQ messages for and store the config for it."""
        lab_id_field = self._message.lab_id
        try:
            self._message.centre_config = next(
                (c for c in self.centres if c[CENTRE_KEY_LAB_ID_DEFAULT] == lab_id_field.value)
            )
        except StopIteration:
            self._message.add_error(
                CreatePlateError(
                    type=ErrorType.CentreNotConfigured,
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
                    description=(
                        f"The lab ID provided '{lab_id_field.value}' "
                        "is not configured to receive messages via RabbitMQ."
                    ),
                    field=lab_id_field.name,
                )
            )

    def _validate_plate(self):
        """Perform validation of the plate field in the message values for sanity.
        This does not check that the message can be inserted into the relevant databases.
        """
        # Ensure that the plate barcode isn't an empty string.
        plate_barcode_field = self._message.plate_barcode
        if not plate_barcode_field.value:
            self._message.add_error(
                CreatePlateError(
                    type=ErrorType.UnpopulatedField,
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
                    description=f"Value for field '{plate_barcode_field.name}' has not been populated.",
                    field=plate_barcode_field.name,
                )
            )

    def _validate_samples(self):
        """Perform validation of the samples array in the message.
        This does not check that the message can be inserted into the relevant databases.
        """
        if self._message.total_samples == 0:
            return

        sample_uuid_field_name = self._message.samples.value[0].sample_uuid.name
        for sample_uuid in self._message.duplicated_sample_values[sample_uuid_field_name]:
            self._message.add_error(
                CreatePlateError(
                    type=ErrorType.NonUniqueValue,
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                    description=f"Sample UUID {sample_uuid} exists more than once in the message.",
                    sample_uuid=sample_uuid,
                    field=sample_uuid_field_name,
                )
            )

        self._message.validated_samples = 0
        for sample in self._message.samples.value:
            if self._validate_sample(sample):
                self._message.validated_samples += 1

    def _validate_sample_field_populated(self, field, sample_uuid):
        if not field.value:
            self._message.add_error(
                CreatePlateError(
                    type=ErrorType.UnpopulatedField,
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                    description=f"Value for field '{field.name}' on sample '{sample_uuid}' has not been populated.",
                    sample_uuid=sample_uuid,
                    field=field.name,
                )
            )

            return False

        return True

    def _validate_sample_field_unique(self, field, sample_uuid, normalise_func=None):
        normalised_value = field.value
        if normalise_func is not None:
            normalised_value = normalise_func(normalised_value)

        if normalised_value in self._message.duplicated_sample_values[field.name]:
            self._message.add_error(
                CreatePlateError(
                    type=ErrorType.NonUniqueValue,
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                    description=(
                        f"Field '{field.name}' on sample '{sample_uuid}' contains the value '{field.value}' "
                        "which is used in more than one sample but should be unique."
                    ),
                    sample_uuid=sample_uuid,
                    field=field.name,
                )
            )

            return False

        return True

    def _validate_sample_field_matches_regex(self, regex, field, sample_uuid):
        if not regex.match(field.value):
            self._message.add_error(
                CreatePlateError(
                    type=ErrorType.InvalidFormatValue,
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                    description=(
                        f"Field '{field.name}' on sample '{sample_uuid}' contains the value '{field.value}' "
                        "which doesn't match the expected format for values in this field."
                    ),
                    sample_uuid=sample_uuid,
                    field=field.name,
                )
            )

            return False

        return True

    def _validate_sample_field_no_later_than(self, timestamp, field, sample_uuid):
        if field.value > timestamp:
            self._message.add_error(
                CreatePlateError(
                    type=ErrorType.OutOfRangeValue,
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                    description=(
                        f"Field '{field.name}' on sample '{sample_uuid}' contains the value '{field.value}' "
                        f"which is too recent and should be lower than '{timestamp}'."
                    ),
                    sample_uuid=sample_uuid,
                    field=field.name,
                )
            )

            return False

        return True

    def _validate_sample(self, sample):
        """Perform validation of complete and consistent data for a single sample in the message."""
        valid = True

        # Ensure the sample UUID is unique
        sample_uuid_field = sample.sample_uuid
        sample_uuid = sample_uuid_field.value
        if sample_uuid in self._message.duplicated_sample_values[sample_uuid_field.name]:
            valid = False

        # Validate root sample ID
        root_sample_id_field = sample.root_sample_id
        if not self._validate_sample_field_populated(
            root_sample_id_field, sample_uuid
        ) or not self._validate_sample_field_unique(root_sample_id_field, sample_uuid):
            valid = False

        # Validate RNA ID
        rna_id_field = sample.rna_id
        if not self._validate_sample_field_populated(
            rna_id_field, sample_uuid
        ) or not self._validate_sample_field_unique(rna_id_field, sample_uuid):
            valid = False

        # Validate COG UK ID
        cog_uk_id_field = sample.cog_uk_id
        if not self._validate_sample_field_populated(
            cog_uk_id_field, sample_uuid
        ) or not self._validate_sample_field_unique(cog_uk_id_field, sample_uuid):
            valid = False

        # Validate plate coordinates
        plate_coordinate_field = sample.plate_coordinate
        if not self._validate_sample_field_matches_regex(
            re.compile(r"^[A-H](?:0?[1-9]|1[0-2])$"),  # A1 - H12 or A01 padded format
            plate_coordinate_field,
            sample_uuid,
        ) or not self._validate_sample_field_unique(plate_coordinate_field, sample_uuid, normalise_plate_coordinate):
            valid = False

        # Validate tested date is not newer than the message create date
        message_create_date = self._message.message_create_date.value
        tested_date_field = sample.tested_date
        if not self._validate_sample_field_no_later_than(message_create_date, tested_date_field, sample_uuid):
            valid = False

        return valid
