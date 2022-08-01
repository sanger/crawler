import logging

from crawler.constants import RABBITMQ_UPDATE_FEEDBACK_ORIGIN_FIELD
from crawler.helpers.general_helpers import extract_duplicated_values
from crawler.rabbit.messages.update_sample_message import ErrorType, UpdateSampleError

LOGGER = logging.getLogger(__name__)


class UpdateSampleValidator:
    def __init__(self, message):
        self._message = message

    def validate(self):
        LOGGER.debug("Starting validation of update message.")

        self._validate_updated_fields()

        LOGGER.debug("Validation finished.")
        self._message.log_error_count()

    def _validate_updated_fields(self):
        """Perform validation that updated fields are populated and only contain each possible field once at most."""
        updated_fields = self._message.updated_fields
        field_names = [x.name for x in updated_fields.value]

        if len(field_names) == 0:
            self._message.add_error(
                UpdateSampleError(
                    type=ErrorType.ValidationUnpopulatedField,
                    origin=RABBITMQ_UPDATE_FEEDBACK_ORIGIN_FIELD,
                    description="No fields to update have been given.",
                    field=updated_fields.name,
                )
            )
            return

        for duped_name in extract_duplicated_values(field_names):
            self._message.add_error(
                UpdateSampleError(
                    type=ErrorType.ValidationNonUniqueFieldName,
                    origin=RABBITMQ_UPDATE_FEEDBACK_ORIGIN_FIELD,
                    description=f"Field with name '{duped_name}' exists more than once in the fields to update.",
                    field=updated_fields.name,
                )
            )
