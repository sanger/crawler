import logging
from datetime import datetime
from typing import NamedTuple, Optional

from pymongo.client_session import ClientSession
from pymongo.database import Database
from pymongo.errors import BulkWriteError

from crawler.constants import (
    CENTRE_KEY_NAME,
    COLLECTION_IMPORTS,
    COLLECTION_SAMPLES,
    COLLECTION_SOURCE_PLATES,
    FIELD_BARCODE,
    FIELD_COORDINATE,
    FIELD_CREATED_AT,
    FIELD_LH_SAMPLE_UUID,
    FIELD_LH_SOURCE_PLATE_UUID,
    FIELD_MONGO_COG_UK_ID,
    FIELD_MONGO_DATE_TESTED,
    FIELD_MONGO_FILTERED_POSITIVE,
    FIELD_MONGO_LAB_ID,
    FIELD_MONGO_MESSAGE_UUID,
    FIELD_MONGO_RESULT,
    FIELD_MONGO_RNA_ID,
    FIELD_MONGO_ROOT_SAMPLE_ID,
    FIELD_MONGO_SAMPLE_INDEX,
    FIELD_MUST_SEQUENCE,
    FIELD_PLATE_BARCODE,
    FIELD_PREFERENTIALLY_SEQUENCE,
    FIELD_SOURCE,
    FIELD_UPDATED_AT,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
)
from crawler.db.mongo import create_import_record, create_mongo_client, get_mongo_collection, get_mongo_db
from crawler.exceptions import TransientRabbitError
from crawler.helpers.general_helpers import create_source_plate_doc
from crawler.helpers.sample_data_helpers import normalise_plate_coordinate
from crawler.rabbit.messages.create_plate_message import CreatePlateError, ErrorType

LOGGER = logging.getLogger(__name__)


class ExportResult(NamedTuple):
    success: bool
    create_plate_error: Optional[CreatePlateError]


class CreatePlateExporter:
    def __init__(self, message, config):
        self._message = message
        self._config = config

        self._plate_uuid = None
        self._samples_inserted = 0

    def export_to_mongo(self):
        with self._mongo_db.client.start_session() as session:
            try:
                with session.start_transaction():
                    source_plate_result = self._record_source_plate_in_mongo_db(session)

                    if not source_plate_result.success:
                        self._message.add_error(source_plate_result.create_plate_error)
                        session.abort_transaction()
                        return

                    samples_result = self._record_samples_in_mongo_db(session)

                    if not samples_result.success:
                        self._message.add_error(samples_result.create_plate_error)
                        session.abort_transaction()
                        return

                    session.commit_transaction()
            finally:
                self._mongo_db.client.close()

    def export_to_dart(self):
        try:
            pass  # Do export
        except Exception as ex:
            LOGGER.exception(ex)

    def record_import(self):
        plate_barcode = self._message.plate_barcode.value
        if not plate_barcode:
            # We don't record imports without a plate barcode available. They would be meaningless without the barcode.
            LOGGER.error(
                f"Import record not created for message with UUID '{self._message.message_uuid.value}' "
                "because it doesn't have a plate barcode."
            )
            return

        try:
            imports_collection = get_mongo_collection(self._mongo_db, COLLECTION_IMPORTS)

            create_import_record(
                imports_collection,
                self._message.centre_config,
                self._samples_inserted,
                plate_barcode,
                self._message.textual_errors_summary,
            )
        except Exception as ex:
            LOGGER.exception(ex)

    @property
    def _mongo_db(self) -> Database:
        if not hasattr(self, "__mongo_db"):
            client = create_mongo_client(self._config)
            self.__mongo_db = get_mongo_db(self._config, client)

        return self.__mongo_db

    @property
    def _mongo_sample_docs(self):
        return [self._map_sample_to_mongo(sample, index) for index, sample in enumerate(self._message.samples.value)]

    def _record_source_plate_in_mongo_db(self, session: ClientSession) -> ExportResult:
        """Find an existing plate in MongoDB or add a new one for the plate in the message."""
        try:
            plate_barcode = self._message.plate_barcode.value
            lab_id_field = self._message.lab_id

            session_database = get_mongo_db(self._config, session.client)
            source_plates_collection = get_mongo_collection(session_database, COLLECTION_SOURCE_PLATES)
            mongo_plate = source_plates_collection.find_one(filter={FIELD_BARCODE: plate_barcode}, session=session)

            if mongo_plate is not None:
                # There was a plate in Mongo DB for this field barcode so check that the lab ID matches then return.
                self._plate_uuid = mongo_plate[FIELD_LH_SOURCE_PLATE_UUID]

                if mongo_plate[FIELD_MONGO_LAB_ID] != lab_id_field.value:
                    return ExportResult(
                        success=False,
                        create_plate_error=CreatePlateError(
                            type=ErrorType.ExportingPlateAlreadyExists,
                            origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
                            description=(
                                f"Plate barcode '{plate_barcode}' already exists "
                                f"with a different lab ID: '{mongo_plate[FIELD_MONGO_LAB_ID]}'"
                            ),
                            field=lab_id_field.name,
                        ),
                    )

                return ExportResult(success=True, create_plate_error=None)

            # Create a new plate for this message.
            mongo_plate = create_source_plate_doc(plate_barcode, lab_id_field.value)
            source_plates_collection.insert_one(mongo_plate, session=session)
            self._plate_uuid = mongo_plate[FIELD_LH_SOURCE_PLATE_UUID]

            return ExportResult(success=True, create_plate_error=None)
        except Exception as ex:
            LOGGER.critical(f"Error accessing MongoDB during export of source plate '{plate_barcode}': {ex}")
            LOGGER.exception(ex)

            raise TransientRabbitError(
                f"There was an error updating MongoDB while exporting plate with barcode '{plate_barcode}'."
            )

    def _record_samples_in_mongo_db(self, session: ClientSession) -> ExportResult:
        message_uuid = self._message.message_uuid.value
        LOGGER.debug(
            f"Attempting to insert {self._message.total_samples} "
            f"samples from message with UUID {message_uuid} into mongo..."
        )

        try:
            session_database = get_mongo_db(self._config, session.client)
            samples_collection = get_mongo_collection(session_database, COLLECTION_SAMPLES)
            result = samples_collection.insert_many(documents=self._mongo_sample_docs, ordered=False, session=session)
        except BulkWriteError as ex:
            # TODO Dissect the error to check for DuplicateKeyErrors -- raise if not
            LOGGER.warning("BulkWriteError: Happens when there are duplicate samples being inserted.")
            LOGGER.exception(ex)

            return ExportResult(
                success=False,
                create_plate_error=CreatePlateError(
                    type=ErrorType.ExportingSampleAlreadyExists,
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
                    description=(f"At least one sample in message with UUID '{message_uuid}' already exists"),
                ),
            )
        except Exception as ex:
            LOGGER.critical(f"Error accessing MongoDB during export of samples for message UUID '{message_uuid}': {ex}")
            LOGGER.exception(ex)

            raise TransientRabbitError(
                f"There was an error updating MongoDB while exporting samples for message UUID '{message_uuid}'."
            )

        self._samples_inserted = len(result.inserted_ids)
        LOGGER.info(f"{self._samples_inserted} samples inserted into mongo.")

        return ExportResult(success=True, create_plate_error=None)

    def _map_sample_to_mongo(self, sample, index):
        return {
            FIELD_MONGO_DATE_TESTED: sample.tested_date.value,
            FIELD_MONGO_LAB_ID: self._message.lab_id.value,
            FIELD_MONGO_RNA_ID: sample.rna_id.value,
            FIELD_MONGO_ROOT_SAMPLE_ID: sample.root_sample_id.value,
            FIELD_MONGO_COG_UK_ID: sample.cog_uk_id.value,
            FIELD_MONGO_RESULT: sample.result.value,
            FIELD_SOURCE: self._message.centre_config[CENTRE_KEY_NAME],
            FIELD_PLATE_BARCODE: self._message.plate_barcode.value,
            FIELD_COORDINATE: normalise_plate_coordinate(sample.plate_coordinate.value),
            FIELD_MONGO_SAMPLE_INDEX: index + 1,
            FIELD_MONGO_MESSAGE_UUID: self._message.message_uuid.value,
            FIELD_MONGO_FILTERED_POSITIVE: sample.fit_to_pick.value,
            FIELD_MUST_SEQUENCE: sample.must_sequence.value,
            FIELD_PREFERENTIALLY_SEQUENCE: sample.preferentially_sequence.value,
            FIELD_LH_SAMPLE_UUID: sample.sample_uuid.value,
            FIELD_LH_SOURCE_PLATE_UUID: self._plate_uuid,
            FIELD_CREATED_AT: datetime.utcnow(),
            FIELD_UPDATED_AT: datetime.utcnow(),
        }
