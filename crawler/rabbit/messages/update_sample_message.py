import logging
from dataclasses import dataclass
from enum import IntEnum
from typing import Any, NamedTuple, Optional

from crawler.rabbit.messages.base_message import BaseMessage
from crawler.rabbit.messages.create_feedback_message import CreateFeedbackError

LOGGER = logging.getLogger(__name__)


FIELD_MESSAGE_UUID = "messageUuid"
FIELD_MESSAGE_CREATE_DATE = "messageCreateDateUtc"
FIELD_SAMPLE = "sample"
FIELD_SAMPLE_UUID = "sampleUuid"
FIELD_UPDATED_FIELDS = "updatedFields"
FIELD_NAME = "name"
FIELD_VALUE = "value"


class ErrorType(IntEnum):
    UnhandledProcessingError = 101


class MessageField(NamedTuple):
    name: str
    value: Any


@dataclass
class UpdateSampleError:
    type: ErrorType
    origin: str
    description: str
    field: Optional[str] = None


class UpdateSampleMessage(BaseMessage):
    def __init__(self, body):
        self._body = body

        self._textual_errors = []
        self._feedback_errors = []
        self._updated_fields = None

    @property
    def textual_errors_summary(self):
        error_count = len(self._textual_errors)

        if error_count == 0:
            errors_label = "No errors were"
        elif error_count == 1:
            errors_label = "1 error was"
        else:
            errors_label = f"{error_count} errors were"

        additional_text = " Only the first 5 are shown." if error_count > 5 else ""

        error_list = [f"{errors_label} reported during processing.{additional_text}"] + self._textual_errors[:5]

        return error_list

    @property
    def feedback_errors(self):
        return self._feedback_errors.copy()

    @property
    def has_errors(self):
        return len(self._textual_errors) > 0 or len(self._feedback_errors) > 0

    @property
    def message_uuid(self):
        return MessageField(FIELD_MESSAGE_UUID, self._body[FIELD_MESSAGE_UUID].decode())

    @property
    def message_create_date(self):
        return MessageField(FIELD_MESSAGE_CREATE_DATE, self._body[FIELD_MESSAGE_CREATE_DATE])

    @property
    def sample_uuid(self):
        return MessageField(FIELD_SAMPLE_UUID, self._body[FIELD_SAMPLE][FIELD_SAMPLE_UUID])

    @property
    def updated_fields(self):
        if self._updated_fields is None:
            self._updated_fields = [
                MessageField(name=body[FIELD_NAME], value=body[FIELD_VALUE])
                for body in self._body[FIELD_SAMPLE][FIELD_UPDATED_FIELDS]
            ]

        return MessageField(FIELD_UPDATED_FIELDS, self._updated_fields)

    def add_error(self, create_error):
        LOGGER.error(f"Error in create plate message: {create_error.description}")
        self._textual_errors.append(create_error.description)
        self._feedback_errors.append(
            CreateFeedbackError(
                typeId=int(create_error.type),
                origin=create_error.origin,
                sampleUuid=create_error.sample_uuid,
                field=create_error.field,
                description=create_error.description,
            )
        )
