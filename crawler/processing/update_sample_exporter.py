import logging
from typing import List, NamedTuple

from pymongo.client_session import ClientSession
from pymongo.database import Database

from crawler.constants import (
    COLLECTION_SAMPLES,
    FIELD_LH_SAMPLE_UUID,
    FIELD_PLATE_BARCODE,
    FIELD_UPDATED_AT,
    RABBITMQ_UPDATE_FEEDBACK_ORIGIN_ROOT,
)
from crawler.db.mongo import create_mongo_client, get_mongo_collection, get_mongo_db
from crawler.exceptions import TransientRabbitError
from crawler.rabbit.messages.update_sample_message import ErrorType, UpdateSampleError

LOGGER = logging.getLogger(__name__)


class ExportResult(NamedTuple):
    success: bool
    update_sample_errors: List[UpdateSampleError]


class UpdateSampleExporter:
    def __init__(self, message, config):
        self._message = message
        self._config = config

        self._plate_barcode = None

    def verify_sample_in_mongo(self):
        with self._mongo_db.client.start_session() as session:
            try:
                self._validate_mongo_properties(session)
            finally:
                self._mongo_db.client.close()

    def update_mongo(self):
        with self._mongo_db.client.start_session() as session:
            try:
                with session.start_transaction():
                    # source_plate_result = self._record_source_plate_in_mongo_db(session)

                    # if not source_plate_result.success:
                    #     return self._abort_transaction_with_errors(session, source_plate_result.create_plate_errors)

                    session.commit_transaction()
            finally:
                self._mongo_db.client.close()

    def update_dart(self):
        result = ExportResult(success=True, update_sample_errors=[])
        if not result.success:
            for error in result.update_sample_errors:
                self._message.add_error(error)

    @property
    def _mongo_db(self) -> Database:
        if not hasattr(self, "__mongo_db"):
            client = create_mongo_client(self._config)
            self.__mongo_db = get_mongo_db(self._config, client)

        return self.__mongo_db

    def _validate_mongo_properties(self, session: ClientSession) -> ExportResult:
        try:
            sample_uuid = self._message.sample_uuid
            message_create_date = self._message.message_create_date

            session_database = get_mongo_db(self._config, session.client)
            samples_collection = get_mongo_collection(session_database, COLLECTION_SAMPLES)
            sample = samples_collection.find_one(filter={FIELD_LH_SAMPLE_UUID: sample_uuid.value}, session=session)

            if sample is None:
                self._message.add_error(
                    UpdateSampleError(
                        type=ErrorType.ExporterSampleDoesNotExist,
                        origin=RABBITMQ_UPDATE_FEEDBACK_ORIGIN_ROOT,
                        description=f"Sample with UUID '{sample_uuid.value}' does not exist.",
                        field=sample_uuid.name,
                    )
                )
                return

            if sample[FIELD_UPDATED_AT] > message_create_date.value:
                self._message.add_error(
                    UpdateSampleError(
                        type=ErrorType.ExporterMessageOutOfDate,
                        origin=RABBITMQ_UPDATE_FEEDBACK_ORIGIN_ROOT,
                        description=(
                            f"The sample was last updated at '{sample[FIELD_UPDATED_AT]}' which is more "
                            f"recent than the message creation date '{message_create_date.value}'."
                        ),
                        field=message_create_date.name,
                    )
                )
                return

            self._plate_barcode = sample[FIELD_PLATE_BARCODE]
        except Exception as ex:
            LOGGER.critical(f"Error accessing MongoDB looking up sample with UUID '{sample_uuid.value}': {ex}")
            LOGGER.exception(ex)

            raise TransientRabbitError(
                f"There was an error accessing MongoDB while looking up sample with UUID '{sample_uuid.value}'."
            )
