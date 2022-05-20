import logging
from typing import NamedTuple, Optional

from pymongo.database import Database

from crawler.constants import COLLECTION_IMPORTS, COLLECTION_SOURCE_PLATES
from crawler.constants import FIELD_BARCODE as MONGO_PLATE_BARCODE
from crawler.constants import FIELD_LAB_ID as MONGO_LAB_ID
from crawler.constants import FIELD_LH_SOURCE_PLATE_UUID, RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE
from crawler.db.mongo import create_import_record, create_mongo_client, get_mongo_collection, get_mongo_db
from crawler.exceptions import TransientRabbitError
from crawler.helpers.general_helpers import create_source_plate_doc
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
        try:
            source_plate_result = self._record_source_plate_in_mongo_db()

            if not source_plate_result.success:
                self._message.add_error(source_plate_result.create_plate_error)
                return

            samples_result = self._record_samples_in_mongo_db()

            if not samples_result.success:
                self._message.add_error(samples_result.create_plate_error)
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

    def _record_source_plate_in_mongo_db(self) -> ExportResult:
        """Find an existing plate in MongoDB or add a new one for the plate in the message."""
        try:
            plate_barcode = self._message.plate_barcode.value
            lab_id_field = self._message.lab_id

            source_plates_collection = get_mongo_collection(self._mongo_db, COLLECTION_SOURCE_PLATES)
            mongo_plate = source_plates_collection.find_one(filter={MONGO_PLATE_BARCODE: plate_barcode})

            if mongo_plate is not None:
                # There was a plate in Mongo DB for this field barcode so check that the lab ID matches then return.
                self._plate_uuid = mongo_plate[FIELD_LH_SOURCE_PLATE_UUID]

                if mongo_plate[MONGO_LAB_ID] != lab_id_field.value:
                    return ExportResult(
                        success=False,
                        create_plate_error=CreatePlateError(
                            type=ErrorType.ExportingPlateAlreadyExists,
                            origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
                            description=(
                                f"Plate barcode '{plate_barcode}' already exists "
                                f"with a different lab ID: '{mongo_plate[MONGO_LAB_ID]}'"
                            ),
                            field=lab_id_field.name,
                        ),
                    )

                return ExportResult(success=True, create_plate_error=None)

            # Create a new plate for this message.
            mongo_plate = create_source_plate_doc(plate_barcode, lab_id_field.value)
            source_plates_collection.insert_one(mongo_plate)
            self._plate_uuid = mongo_plate[FIELD_LH_SOURCE_PLATE_UUID]

            return ExportResult(success=True, create_plate_error=None)
        except Exception as ex:
            LOGGER.critical(f"Error accessing MongoDB during export of source plate '{plate_barcode}': {ex}")
            LOGGER.exception(ex)

            raise TransientRabbitError(
                f"There was an error updating MongoDB while exporting plate with barcode '{plate_barcode}'."
            )

    def _record_samples_in_mongo_db(self) -> ExportResult:
        return ExportResult(success=True, create_plate_error=None)
