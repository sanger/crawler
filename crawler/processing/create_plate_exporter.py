import logging

from pymongo.database import Database

from crawler.constants import COLLECTION_SOURCE_PLATES
from crawler.constants import FIELD_BARCODE as MONGO_PLATE_BARCODE
from crawler.constants import FIELD_LAB_ID as MONGO_LAB_ID
from crawler.constants import (
    FIELD_LH_SOURCE_PLATE_UUID,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_EXPORTING,
    RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
)
from crawler.db.mongo import create_mongo_client, get_mongo_collection, get_mongo_db
from crawler.exceptions import Error
from crawler.helpers.general_helpers import create_source_plate_doc
from crawler.rabbit.messages.create_plate_message import CreatePlateError

LOGGER = logging.getLogger(__name__)


class ExportingError(Error):
    def __init__(self, create_plate_error):
        self.create_plate_error = create_plate_error


class CreatePlateExporter:
    def __init__(self, message, config):
        self._message = message
        self._config = config

        self._plate_uuid = None

    def export_to_mongo(self):
        try:
            self._record_source_plate_in_mongo_db()
        except ExportingError as ex:
            # Something that was handled gracefully went wrong. Add the error to the message and stop exporting.
            self._message.add_error(ex.create_plate_error)
        finally:
            self._mongo_db.client.close()

    @property
    def _mongo_db(self) -> Database:
        if not hasattr(self, "__mongo_db"):
            client = create_mongo_client(self._config)
            self.__mongo_db = get_mongo_db(self._config, client)

        return self.__mongo_db

    def _record_source_plate_in_mongo_db(self):
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
                    raise ExportingError(
                        CreatePlateError(
                            origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_PLATE,
                            description=(
                                f"Plate barcode '{plate_barcode}' already exists "
                                f"with a different lab ID: '{mongo_plate[MONGO_LAB_ID]}'"
                            ),
                            field=lab_id_field.name,
                        )
                    )
                return

            # Create a new plate for this message.
            mongo_plate = create_source_plate_doc(plate_barcode, lab_id_field.value)
            source_plates_collection.insert_one(mongo_plate)
            self._plate_uuid = mongo_plate[FIELD_LH_SOURCE_PLATE_UUID]
        except ExportingError:
            # These are handled in the calling method.
            raise
        except Exception as ex:
            LOGGER.critical(f"Error accessing MongoDB during export of source plate '{plate_barcode}': {ex}")
            LOGGER.exception(ex)

            raise ExportingError(
                CreatePlateError(
                    origin=RABBITMQ_CREATE_FEEDBACK_ORIGIN_EXPORTING,
                    description="There was an error updating MongoDB while exporting the plate.",
                    long_description=(
                        f"There was an error updating MongoDB while exporting plate with barcode {plate_barcode}."
                    ),
                )
            )
