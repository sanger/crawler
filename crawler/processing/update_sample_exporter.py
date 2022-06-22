import logging
from datetime import datetime
from http import HTTPStatus
from typing import List, NamedTuple

import requests
from pymongo.client_session import ClientSession
from pymongo.database import Database

from crawler.constants import (
    COLLECTION_SAMPLES,
    DART_STATE_NO_PLATE,
    DART_STATE_PENDING,
    FIELD_LH_SAMPLE_UUID,
    FIELD_MUST_SEQUENCE,
    FIELD_PLATE_BARCODE,
    FIELD_PREFERENTIALLY_SEQUENCE,
    FIELD_UPDATED_AT,
    RABBITMQ_UPDATE_FEEDBACK_ORIGIN_ROOT,
)
from crawler.db.dart import create_dart_sql_server_conn, get_dart_plate_state
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

        self._mongo_sample = None
        self._plate_missing_in_dart = False

    def verify_sample_in_mongo(self):
        try:
            with self._mongo_db.client.start_session() as session:
                self._validate_mongo_properties(session)
        finally:
            self._mongo_db.client.close()

    def verify_plate_state(self):
        self._verify_plate_not_in_cherrytrack() and self._verify_plate_state_in_dart()

    def update_mongo(self):
        try:
            with self._mongo_db.client.start_session() as session:
                self._update_sample_in_mongo(session)
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

    @property
    def _plate_barcode(self):
        try:
            return self._mongo_sample[FIELD_PLATE_BARCODE]
        except (TypeError, KeyError) as ex:
            raise ValueError(
                "No Mongo sample was set -- this probably means verify_sample_in_mongo"
                "was not called first in the exporter."
            ) from ex

    @property
    def _updated_mongo_fields(self):
        field_name_map = {"mustSequence": FIELD_MUST_SEQUENCE, "preferentiallySequence": FIELD_PREFERENTIALLY_SEQUENCE}
        fields = {field_name_map[field.name]: field.value for field in self._message.updated_fields.value}
        fields[FIELD_UPDATED_AT] = datetime.utcnow()

        return fields

    def _validate_mongo_properties(self, session: ClientSession) -> None:
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

            self._mongo_sample = sample
        except Exception as ex:
            LOGGER.critical(f"Error accessing MongoDB looking up sample with UUID '{sample_uuid.value}': {ex}")
            LOGGER.exception(ex)

            raise TransientRabbitError(
                f"There was an error accessing MongoDB while looking up sample with UUID '{sample_uuid.value}'."
            )

    def _add_plate_already_picked_error(self):
        self._message.add_error(
            UpdateSampleError(
                type=ErrorType.ExporterPlateAlreadyPicked,
                origin=RABBITMQ_UPDATE_FEEDBACK_ORIGIN_ROOT,
                description=f"Sample is on plate with barcode '{self._plate_barcode}' which has already been picked.",
            )
        )

    def _verify_plate_not_in_cherrytrack(self):
        LOGGER.info("Checking for source plate in Cherrytrack.")

        try:
            cherrytrack_url = f"{self._config.CHERRYTRACK_BASE_URL}/source-plates/{self._plate_barcode}"
            response = requests.get(cherrytrack_url)

            if response.status_code == HTTPStatus.OK:
                self._add_plate_already_picked_error()
                return False

            return True
        except Exception as ex:
            LOGGER.exception(ex)
            raise TransientRabbitError(
                f"Unable to make a request to Cherrytrack for plate with barcode '{self._plate_barcode}'."
            )

    def _verify_plate_state_in_dart(self):
        LOGGER.info("Checking source plate state in DART.")

        if (sql_server_connection := create_dart_sql_server_conn(self._config)) is None:
            raise TransientRabbitError(
                f"Error connecting to the DART database to check state for plate with barcode '{self._plate_barcode}'."
            )

        try:
            plate_state = get_dart_plate_state(sql_server_connection.cursor(), str(self._plate_barcode))

            if plate_state not in (DART_STATE_NO_PLATE, DART_STATE_PENDING):
                self._add_plate_already_picked_error()
                return False

            if plate_state == DART_STATE_NO_PLATE:
                self._plate_missing_in_dart = True
                LOGGER.critical(
                    f"DART database was queried to check the state of plate with barcode '{self._plate_barcode}' but "
                    "the plate does not exist. Manual transfer of this plate and its samples from Mongo to DART "
                    "will be needed."
                )

            return True
        except Exception as ex:
            LOGGER.exception(ex)
            raise TransientRabbitError(
                f"Error querying the DART database to check state for plate with barcode '{self._plate_barcode}'."
            )
        finally:
            sql_server_connection.close()

    def _update_sample_in_mongo(self, session):
        LOGGER.info("Updating the sample in Mongo.")
        sample_uuid = self._message.sample_uuid.value

        try:
            session_database = get_mongo_db(self._config, session.client)
            samples_collection = get_mongo_collection(session_database, COLLECTION_SAMPLES)
            samples_collection.update_one(
                {FIELD_LH_SAMPLE_UUID: sample_uuid}, {"$set": self._updated_mongo_fields}, session=session
            )
        except Exception as ex:
            LOGGER.critical(f"Error accessing MongoDB during update of sample '{sample_uuid}': {ex}")
            LOGGER.exception(ex)

            raise TransientRabbitError(
                f"There was an error updating MongoDB while update sample with UUID '{sample_uuid}'."
            )
