import logging
from datetime import datetime
from typing import List, NamedTuple

from pymongo.client_session import ClientSession
from pymongo.errors import BulkWriteError

from crawler.constants import (
    CENTRE_KEY_BIOMEK_LABWARE_CLASS,
    CENTRE_KEY_NAME,
    COLLECTION_IMPORTS,
    COLLECTION_SAMPLES,
    COLLECTION_SOURCE_PLATES,
    DART_STATE_PENDING,
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
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_ROOT,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
)
from crawler.db.dart import (
    add_dart_plate_if_doesnt_exist,
    add_dart_well_properties_if_positive,
    create_dart_sql_server_conn,
)
from crawler.db.mongo import create_mongo_client, get_mongo_collection, get_mongo_db
from crawler.exceptions import TransientRabbitError
from crawler.helpers.db_helpers import create_mongo_import_record
from crawler.helpers.general_helpers import create_source_plate_doc
from crawler.helpers.sample_data_helpers import normalise_plate_coordinate
from crawler.rabbit.messages.parsers.create_plate_message import CreatePlateError, ErrorType

LOGGER = logging.getLogger(__name__)


class ExportResult(NamedTuple):
    success: bool
    create_plate_errors: List[CreatePlateError]


class CreatePlateExporter:
    def __init__(self, message, config):
        self._message = message
        self._config = config

        self._plate_uuid = None
        self._samples_inserted = 0
        self.__mongo_db = None

    def export_to_mongo(self):
        with self._mongo_db.client.start_session() as session:
            with session.start_transaction():
                source_plate_result = self._record_source_plate_in_mongo_db(session)

                if not source_plate_result.success:
                    return self._abort_transaction_with_errors(session, source_plate_result.create_plate_errors)

                samples_result = self._record_samples_in_mongo_db(session)

                if not samples_result.success:
                    return self._abort_transaction_with_errors(session, samples_result.create_plate_errors)

                session.commit_transaction()

    def export_to_dart(self):
        result = self._record_samples_in_dart()
        if not result.success:
            for error in result.create_plate_errors:
                self._message.add_error(error)

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

            create_mongo_import_record(
                imports_collection,
                self._message.centre_config,
                self._samples_inserted,
                plate_barcode,
                self._message.textual_errors_summary,
            )
        except Exception as ex:
            LOGGER.exception(ex)

    @property
    def _mongo_db(self):
        if self.__mongo_db is None:
            client = create_mongo_client(self._config)
            self.__mongo_db = get_mongo_db(self._config, client)

        return self.__mongo_db

    @property
    def _mongo_sample_docs(self):
        return [self._map_sample_to_mongo(sample, index) for index, sample in enumerate(self._message.samples.value)]

    def _abort_transaction_with_errors(self, session, errors):
        for error in errors:
            self._message.add_error(error)
        session.abort_transaction()

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
                        create_plate_errors=[
                            CreatePlateError(
                                type=ErrorType.ExportingPlateAlreadyExists,
                                origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
                                description=(
                                    f"Plate barcode '{plate_barcode}' already exists "
                                    f"with a different lab ID: '{mongo_plate[FIELD_MONGO_LAB_ID]}'"
                                ),
                                field=lab_id_field.name,
                            )
                        ],
                    )

                return ExportResult(success=True, create_plate_errors=[])

            # Create a new plate for this message.
            mongo_plate = create_source_plate_doc(plate_barcode, lab_id_field.value)
            source_plates_collection.insert_one(mongo_plate, session=session)
            self._plate_uuid = mongo_plate[FIELD_LH_SOURCE_PLATE_UUID]

            return ExportResult(success=True, create_plate_errors=[])
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
            try:
                session_database = get_mongo_db(self._config, session.client)
                samples_collection = get_mongo_collection(session_database, COLLECTION_SAMPLES)
                result = samples_collection.insert_many(
                    documents=self._mongo_sample_docs, ordered=False, session=session
                )
            except BulkWriteError as ex:
                LOGGER.warning("BulkWriteError: will now establish whether this was because of duplicate samples.")

                duplication_errors = list(
                    filter(lambda x: x["code"] == 11000, ex.details["writeErrors"])  # type: ignore
                )

                if len(duplication_errors) == 0:
                    # There weren't any duplication errors so this is not a problem with the message contents!
                    raise

                create_plate_errors = []
                for duplicate in [x["op"] for x in duplication_errors]:
                    create_plate_errors.append(
                        CreatePlateError(
                            type=ErrorType.ExportingSampleAlreadyExists,
                            origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_SAMPLE,
                            description=(
                                f"Sample with UUID '{duplicate[FIELD_LH_SAMPLE_UUID]}' was unable to be inserted "
                                "because another sample already exists with "
                                f"Lab ID = '{duplicate[FIELD_MONGO_LAB_ID]}'; "
                                f"Root Sample ID = '{duplicate[FIELD_MONGO_ROOT_SAMPLE_ID]}'; "
                                f"RNA ID = '{duplicate[FIELD_MONGO_RNA_ID]}'; "
                                f"Result = '{duplicate[FIELD_MONGO_RESULT]}'"
                            ),
                            sample_uuid=duplicate[FIELD_LH_SAMPLE_UUID],
                        )
                    )

                return ExportResult(success=False, create_plate_errors=create_plate_errors)
        except Exception as ex:
            LOGGER.critical(f"Error accessing MongoDB during export of samples for message UUID '{message_uuid}': {ex}")
            LOGGER.exception(ex)

            raise TransientRabbitError(
                f"There was an error updating MongoDB while exporting samples for message UUID '{message_uuid}'."
            )

        self._samples_inserted = len(result.inserted_ids)
        LOGGER.info(f"{self._samples_inserted} samples inserted into mongo.")

        return ExportResult(success=True, create_plate_errors=[])

    def _map_sample_to_mongo(self, sample, index):
        return {
            FIELD_MONGO_DATE_TESTED: sample.tested_date.value,
            FIELD_MONGO_LAB_ID: self._message.lab_id.value,
            FIELD_MONGO_RNA_ID: sample.rna_id.value,
            FIELD_MONGO_ROOT_SAMPLE_ID: sample.root_sample_id.value,
            FIELD_MONGO_COG_UK_ID: sample.cog_uk_id.value,
            FIELD_MONGO_RESULT: sample.result.value.capitalize(),
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

    def _record_samples_in_dart(self):
        def export_result_with_error(error_description):
            LOGGER.critical(error_description)

            return ExportResult(
                success=False,
                create_plate_errors=[
                    CreatePlateError(
                        type=ErrorType.ExportingPostFeedback,  # This error will only reach the imports record
                        origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_ROOT,
                        description=error_description,
                    )
                ],
            )

        LOGGER.info("Adding to DART")
        message_uuid = self._message.message_uuid.value
        plate_barcode = self._message.plate_barcode.value

        if (sql_server_connection := create_dart_sql_server_conn(self._config)) is None:
            return export_result_with_error(
                f"Error connecting to DART database for plate with barcode '{plate_barcode}' "
                f"in message with UUID '{message_uuid}'"
            )

        try:
            cursor = sql_server_connection.cursor()

            plate_state = add_dart_plate_if_doesnt_exist(
                cursor, plate_barcode, self._message.centre_config[CENTRE_KEY_BIOMEK_LABWARE_CLASS]
            )

            if plate_state == DART_STATE_PENDING:
                for sample in self._mongo_sample_docs:
                    add_dart_well_properties_if_positive(cursor, sample, plate_barcode)

            cursor.commit()

            LOGGER.debug(
                f"DART database inserts completed successfully for plate with barcode '{plate_barcode}' "
                "in message with UUID '{message_uuid}'"
            )
            return ExportResult(success=True, create_plate_errors=[])
        except Exception as ex:
            LOGGER.exception(ex)

            # Rollback statements executed since previous commit/rollback
            cursor.rollback()

            return export_result_with_error(
                f"DART database inserts failed for plate with barcode '{plate_barcode}' "
                f"in message with UUID '{message_uuid}'"
            )
        finally:
            sql_server_connection.close()
