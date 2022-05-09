import logging

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


class CreatePlateMessage:
    def __init__(self, body):
        self._body = body

        self.validated_samples = 0
        self._errors = []

    @property
    def errors(self):
        return self._errors.copy()

    @property
    def total_samples(self):
        return len(self._body[FIELD_PLATE][FIELD_SAMPLES])

    @property
    def message_uuid(self):
        return self._body[FIELD_MESSAGE_UUID].decode()

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
