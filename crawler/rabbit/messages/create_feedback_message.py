from typing import List, TypedDict

from typing_extensions import NotRequired


class CreateFeedbackError(TypedDict):
    typeId: int
    origin: str
    sampleUuid: NotRequired[str]
    field: NotRequired[str]
    description: str


class CreateFeedbackMessage(TypedDict):
    sourceMessageUuid: str
    countOfTotalSamples: int
    countOfValidSamples: int
    operationWasErrorFree: bool
    errors: List[CreateFeedbackError]
