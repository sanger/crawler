import logging
from email.message import Message
from typing import Any, NamedTuple

LOGGER = logging.getLogger(__name__)


FIELD_SOURCE_MESSAGE_UUID = "sourceMessageUuid"
FIELD_COUNT_OF_TOTAL_SAMPLES = "countOfTotalSamples"
FIELD_COUNT_OF_VALID_SAMPLES = "countOfValidSamples"
FIELD_OPERATION_WAS_ERROR_FREE = "operationWasErrorFree"
FIELD_ERRORS_LIST = "errors"
FIELD_ERROR_TYPE_ID = "typeId"
FIELD_ERROR_ORIGIN = "origin"
FIELD_ERROR_SAMPLE_UUID = "sampleUuid"
FIELD_ERROR_FIELD_NAME = "field"
FIELD_ERROR_DESCRIPTION = "description"


class MessageField(NamedTuple):
    name: str
    value: Any


class CreatePlateFeedbackError:
    def __init__(self, body):
        self._body = body

    @property
    def type_id(self):
        return MessageField(FIELD_ERROR_TYPE_ID, self._body[FIELD_ERROR_TYPE_ID])

    @property
    def origin(self):
        return MessageField(FIELD_ERROR_ORIGIN, self._body[FIELD_ERROR_ORIGIN])

    @property
    def sample_uuid(self):
        try:
            return MessageField(FIELD_ERROR_SAMPLE_UUID, self._body[FIELD_ERROR_SAMPLE_UUID])
        except KeyError:
            return MessageField(FIELD_ERROR_SAMPLE_UUID, None)

    @property
    def field_name(self):
        try:
            return MessageField(FIELD_ERROR_FIELD_NAME, self._body[FIELD_ERROR_FIELD_NAME])
        except KeyError:
            return MessageField(FIELD_ERROR_FIELD_NAME, None)

    @property
    def description(self):
        return MessageField(FIELD_ERROR_DESCRIPTION, self._body[FIELD_ERROR_DESCRIPTION])


class CreatePlateFeedbackMessage:
    def __init__(self, body):
        super().__init__()
        self._body = body

        self._errors = None

    @property
    def source_message_uuid(self):
        return MessageField(FIELD_SOURCE_MESSAGE_UUID, self._body[FIELD_SOURCE_MESSAGE_UUID])

    @property
    def count_of_total_samples(self):
        return MessageField(FIELD_COUNT_OF_TOTAL_SAMPLES, self._body[FIELD_COUNT_OF_TOTAL_SAMPLES])

    @property
    def count_of_valid_samples(self):
        return MessageField(FIELD_COUNT_OF_VALID_SAMPLES, self._body[FIELD_COUNT_OF_VALID_SAMPLES])

    @property
    def operation_was_error_free(self):
        return MessageField(FIELD_OPERATION_WAS_ERROR_FREE, self._body[FIELD_OPERATION_WAS_ERROR_FREE])

    @property
    def errors(self):
        if self._errors is None:
            self._errors = [CreatePlateFeedbackError(body) for body in self._body[FIELD_ERRORS_LIST]]

        return MessageField(FIELD_ERRORS_LIST, self._errors)
