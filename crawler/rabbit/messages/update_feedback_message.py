from typing import List, TypedDict

from typing_extensions import NotRequired


class UpdateFeedbackError(TypedDict):
    typeId: int
    origin: str
    field: NotRequired[str]
    description: str


class UpdateFeedbackMessage(TypedDict):
    sourceMessageUuid: str
    operationWasErrorFree: bool
    errors: List[UpdateFeedbackError]
