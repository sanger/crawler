import logging
from typing import Any, NamedTuple

from crawler.helpers.general_helpers import extract_duplicated_values as extract_dupes
from crawler.helpers.sample_data_helpers import normalise_plate_coordinate
from crawler.rabbit.messages.create_feedback_message import CreateFeedbackError

LOGGER = logging.getLogger(__name__)


FIELD_COG_UK_ID = "cogUkId"
FIELD_FIT_TO_PICK = "fitToPick"
FIELD_LAB_ID = "labId"
FIELD_MESSAGE_CREATE_DATE = "messageCreateDateUtc"
FIELD_MESSAGE_UUID = "messageUuid"
FIELD_MUST_SEQUENCE = "mustSequence"
FIELD_PLATE = "plate"
FIELD_PLATE_BARCODE = "plateBarcode"
FIELD_PLATE_COORDINATE = "plateCoordinate"
FIELD_PREFERENTIALLY_SEQUENCE = "preferentiallySequence"
FIELD_RESULT = "result"
FIELD_RNA_ID = "rnaId"
FIELD_ROOT_SAMPLE_ID = "rootSampleId"
FIELD_SAMPLE_UUID = "sampleUuid"
FIELD_SAMPLES = "samples"
FIELD_TESTED_DATE = "testedDateUtc"


class MessageField(NamedTuple):
    name: str
    value: Any


class CreatePlateSample:
    def __init__(self, body):
        self._body = body

    @property
    def cog_uk_id(self):
        return MessageField(FIELD_COG_UK_ID, self._body[FIELD_COG_UK_ID])

    @property
    def fit_to_pick(self):
        return MessageField(FIELD_FIT_TO_PICK, self._body[FIELD_FIT_TO_PICK])

    @property
    def must_sequence(self):
        return MessageField(FIELD_MUST_SEQUENCE, self._body[FIELD_MUST_SEQUENCE])

    @property
    def plate_coordinate(self):
        return MessageField(FIELD_PLATE_COORDINATE, self._body[FIELD_PLATE_COORDINATE])

    @property
    def preferentially_sequence(self):
        return MessageField(FIELD_PREFERENTIALLY_SEQUENCE, self._body[FIELD_PREFERENTIALLY_SEQUENCE])

    @property
    def result(self):
        return MessageField(FIELD_RESULT, self._body[FIELD_RESULT])

    @property
    def rna_id(self):
        return MessageField(FIELD_RNA_ID, self._body[FIELD_RNA_ID])

    @property
    def root_sample_id(self):
        return MessageField(FIELD_ROOT_SAMPLE_ID, self._body[FIELD_ROOT_SAMPLE_ID])

    @property
    def sample_uuid(self):
        return MessageField(FIELD_SAMPLE_UUID, self._body[FIELD_SAMPLE_UUID].decode())

    @property
    def tested_date(self):
        return MessageField(FIELD_TESTED_DATE, self._body[FIELD_TESTED_DATE])


class CreatePlateMessage:
    def __init__(self, body):
        self._body = body

        self.validated_samples = 0
        self._errors = []

        self._duplicated_sample_values = None
        self._samples = None

    @property
    def errors(self):
        return self._errors.copy()

    @property
    def total_samples(self):
        return len(self._body[FIELD_PLATE][FIELD_SAMPLES])

    @property
    def message_uuid(self):
        return MessageField(FIELD_MESSAGE_UUID, self._body[FIELD_MESSAGE_UUID].decode())

    @property
    def message_create_date(self):
        return MessageField(FIELD_MESSAGE_CREATE_DATE, self._body[FIELD_MESSAGE_CREATE_DATE])

    @property
    def lab_id(self):
        return MessageField(FIELD_LAB_ID, self._body[FIELD_PLATE][FIELD_LAB_ID])

    @property
    def plate_barcode(self):
        return MessageField(FIELD_PLATE_BARCODE, self._body[FIELD_PLATE][FIELD_PLATE_BARCODE])

    @property
    def samples(self):
        if self._samples is None:
            self._samples = [CreatePlateSample(body) for body in self._body[FIELD_PLATE][FIELD_SAMPLES]]

        return MessageField(FIELD_SAMPLES, self._samples)

    @property
    def duplicated_sample_values(self):
        if self._duplicated_sample_values is None:
            self._duplicated_sample_values = {
                FIELD_SAMPLE_UUID: extract_dupes([s.sample_uuid.value for s in self.samples.value]),
                FIELD_ROOT_SAMPLE_ID: extract_dupes([s.root_sample_id.value for s in self.samples.value]),
                FIELD_RNA_ID: extract_dupes([s.rna_id.value for s in self.samples.value]),
                FIELD_COG_UK_ID: extract_dupes([s.cog_uk_id.value for s in self.samples.value]),
                FIELD_PLATE_COORDINATE: extract_dupes(
                    [normalise_plate_coordinate(s.plate_coordinate.value) for s in self.samples.value]
                ),
            }

        return self._duplicated_sample_values

    def add_error(self, origin, description, sample_uuid="", field=""):
        LOGGER.error(
            f"Error found in message with origin '{origin}', sampleUuid '{sample_uuid}', field '{field}': {description}"
        )
        self._errors.append(
            CreateFeedbackError(
                origin=origin,
                sampleUuid=sample_uuid,
                field=field,
                description=description,
            )
        )
