from typing import List, TypedDict


class CreateFeedbackError(TypedDict):
    origin: str
    sampleUuid: str
    field: str
    description: str


class CreateFeedbackMessage(TypedDict):
    sourceMessageUuid: str
    countOfTotalSamples: int
    countOfValidSamples: int
    operationWasErrorFree: bool
    errors: List[CreateFeedbackError]
