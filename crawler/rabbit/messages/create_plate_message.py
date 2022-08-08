from datetime import datetime
from typing import List, TypedDict


class Sample(TypedDict):
    sampleUuid: bytes
    rootSampleId: str
    rnaId: str
    cogUkId: str
    plateCoordinate: str
    preferentiallySequence: bool
    mustSequence: bool
    fitToPick: bool
    result: str
    testedDateUtc: datetime


class Plate(TypedDict):
    labId: str
    plateBarcode: str
    samples: List[Sample]


class CreatePlateMessage(TypedDict):
    messageUuid: bytes
    messageCreateDateUtc: datetime
    plate: Plate
