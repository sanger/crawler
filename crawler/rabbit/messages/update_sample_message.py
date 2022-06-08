import logging
from dataclasses import dataclass
from enum import IntEnum
from typing import Any, NamedTuple, Optional

from crawler.rabbit.messages.base_message import BaseMessage
from crawler.rabbit.messages.update_feedback_message import UpdateFeedbackError

LOGGER = logging.getLogger(__name__)


FIELD_MESSAGE_CREATE_DATE = "messageCreateDateUtc"
FIELD_MESSAGE_UUID = "messageUuid"
FIELD_NAME = "name"
FIELD_SAMPLE = "sample"
FIELD_SAMPLE_UUID = "sampleUuid"
FIELD_UPDATED_FIELDS = "updatedFields"
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
        super().__init__()
        self._body = body

        self._feedback_errors = []
        self._updated_fields = None

    @property
    def feedback_errors(self):
        return self._feedback_errors.copy()

    @property
    def has_errors(self):
        return len(self._feedback_errors) > 0

    @property
    def message_uuid(self):
        return MessageField(FIELD_MESSAGE_UUID, self._body[FIELD_MESSAGE_UUID].decode())

    @property
    def message_create_date(self):
        return MessageField(FIELD_MESSAGE_CREATE_DATE, self._body[FIELD_MESSAGE_CREATE_DATE])

    @property
    def sample_uuid(self):
        return MessageField(FIELD_SAMPLE_UUID, self._body[FIELD_SAMPLE][FIELD_SAMPLE_UUID].decode())

    @property
    def updated_fields(self):
        if self._updated_fields is None:
            self._updated_fields = [
                MessageField(name=body[FIELD_NAME], value=body[FIELD_VALUE])
                for body in self._body[FIELD_SAMPLE][FIELD_UPDATED_FIELDS]
            ]

        return MessageField(FIELD_UPDATED_FIELDS, self._updated_fields)

    def add_error(self, update_error):
        LOGGER.error(f"Error in create plate message: {update_error.description}")
        self.add_textual_error(update_error.description)
        self._feedback_errors.append(
            UpdateFeedbackError(
                typeId=int(update_error.type),
                origin=update_error.origin,
                field=update_error.field,
                description=update_error.description,
            )
        )
