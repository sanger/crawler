import logging
from dataclasses import dataclass
from enum import Enum, auto
from time import sleep, time
from typing import Optional

from lab_share_lib.config_readers import get_basic_publisher, get_rabbit_server_details, get_redpanda_schema_registry
from lab_share_lib.processing.rabbit_message import RabbitMessage
from lab_share_lib.rabbit.avro_encoder import AvroEncoder
from lab_share_lib.rabbit.basic_publisher import BasicPublisher
from lab_share_lib.rabbit.schema_registry import SchemaRegistry

from crawler.constants import (
    RABBITMQ_ROUTING_KEY_CREATE_PLATE,
    RABBITMQ_SUBJECT_CREATE_PLATE,
    RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK,
    TEST_DATA_ERROR_PLATE_CREATION_FAILED,
)
from crawler.exceptions import CherrypickerDataError
from crawler.rabbit.basic_getter import BasicGetter, FetchedMessage
from crawler.rabbit.messages.parsers.create_plate_feedback_message import CreatePlateFeedbackMessage
from crawler.rabbit.messages.parsers.create_plate_message import FIELD_MESSAGE_UUID
from crawler.types import Config

LOGGER = logging.getLogger(__name__)


class MessageStatus(Enum):
    DISCARD = auto()
    HAS_ERRORS = auto()
    VALID = auto()


@dataclass
class ParseResult:
    message_status: MessageStatus
    source_uuid: Optional[str] = None


class CPTDProcessor:
    def __init__(self, config: Config):
        self._config = config

        self.__schema_registry: Optional[SchemaRegistry] = None
        self.__basic_publisher: Optional[BasicPublisher] = None
        self.__encoder: Optional[AvroEncoder] = None
        self.__decoder: Optional[AvroEncoder] = None

    def process(self, create_plate_messages: list) -> None:
        """Send the plate messages to RabbitMQ then poll for the feedback messages indicating they were all processed
        correctly.  If there are any issues doing this, raises a CherrypickerDataError to populate the API response
        with.

        Arguments:
           create_plate_messages {list} -- a list of pre-prepared create plate messages to generate the test data with.
        """
        LOGGER.info("Starting generation of cherrypicker test data via RabbitMQ.")
        self._publish_messages(create_plate_messages)
        self._ensure_no_feedback_errors([message[FIELD_MESSAGE_UUID].decode() for message in create_plate_messages])
        LOGGER.info("Test data generation completed successfully.")

    @property
    def _schema_registry(self) -> SchemaRegistry:
        if self.__schema_registry is None:
            self.__schema_registry = get_redpanda_schema_registry(self._config)

        return self.__schema_registry

    @property
    def _basic_publisher(self) -> BasicPublisher:
        if self.__basic_publisher is None:
            self.__basic_publisher = get_basic_publisher(
                self._config, self._config.RABBITMQ_CPTD_USERNAME, self._config.RABBITMQ_CPTD_PASSWORD
            )

        return self.__basic_publisher

    @property
    def _encoder(self) -> AvroEncoder:
        if self.__encoder is None:
            self.__encoder = AvroEncoder(self._schema_registry, RABBITMQ_SUBJECT_CREATE_PLATE)

        return self.__encoder

    @property
    def _decoder(self) -> AvroEncoder:
        if self.__decoder is None:
            self.__decoder = AvroEncoder(self._schema_registry, RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK)

        return self.__decoder

    def _publish_messages(self, create_plate_messages: list) -> None:
        LOGGER.info("Publishing create plate messages for cherrypicker test data.")
        for message in create_plate_messages:
            encoded_message = self._encoder.encode([message])
            self._basic_publisher.publish_message(
                self._config.RABBITMQ_CPTD_CRUD_EXCHANGE,
                RABBITMQ_ROUTING_KEY_CREATE_PLATE,
                encoded_message.body,
                RABBITMQ_SUBJECT_CREATE_PLATE,
                encoded_message.version,
            )

    def _ensure_no_feedback_errors(self, message_uuids: list) -> None:
        """Repeatedly read messages from the CPTD feedback queue and cross reference with the provided list of
        message UUIDs. Once all UUIDs have had feedback without errors, this method returns.

        - If any feedback for an expected UUID has errors, raises a CherrypickerDataError with a message to try again.
        - If any of the UUIDs don't receive feedback within a reasonable time frame, a CherrypickerDataError will be
          raised with a message to try again.

        Arguments:
           message_uuids {list} -- a list of message UUIDs to look for feedback on.
        """
        LOGGER.info(
            "Beginning loop of feedback parsing for cherrypicker test data. Loop will run continuously until no "
            f"message is seen for {self._config.CPTD_FEEDBACK_WAIT_TIME} seconds."
        )
        unconfirmed_uuids = message_uuids.copy()
        t_end = time() + self._config.CPTD_FEEDBACK_WAIT_TIME

        with BasicGetter(
            get_rabbit_server_details(
                self._config, self._config.RABBITMQ_CPTD_USERNAME, self._config.RABBITMQ_CPTD_PASSWORD
            )
        ) as basic_getter:
            while time() < t_end and len(unconfirmed_uuids) > 0:
                fetched_message = basic_getter.get_message(self._config.RABBITMQ_CPTD_FEEDBACK_QUEUE)
                if fetched_message is None:
                    sleep(1)
                    continue  # There was no message on the queue

                t_end = time() + self._config.CPTD_FEEDBACK_WAIT_TIME  # Extend our loop time

                parse_result = self._parse_feedback_message(fetched_message, unconfirmed_uuids)

                if parse_result.message_status == MessageStatus.VALID:
                    unconfirmed_uuids.remove(parse_result.source_uuid)
                elif parse_result.message_status == MessageStatus.HAS_ERRORS:
                    break

        if len(unconfirmed_uuids) > 0:
            LOGGER.error(
                "There were UUIDs that were submitted to the CRUD queue but were not resolved in the feedback queue."
            )
            raise CherrypickerDataError(TEST_DATA_ERROR_PLATE_CREATION_FAILED)

    def _parse_feedback_message(self, fetched_message: FetchedMessage, expected_uuids: list) -> ParseResult:
        rabbit_message = RabbitMessage(fetched_message.headers, fetched_message.body)
        if rabbit_message.subject != RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK:
            LOGGER.debug(f"Fetched message has incorrect subject '{rabbit_message.subject}'. Discarding message.")
            return ParseResult(MessageStatus.DISCARD)  # We can only process create plate feedback messages

        rabbit_message.decode(self._decoder)
        if not rabbit_message.contains_single_message:
            LOGGER.debug("Fetched message contains more than one feedback message. Discarding message.")
            return ParseResult(MessageStatus.DISCARD)  # We can only process single messages

        feedback_message = CreatePlateFeedbackMessage(rabbit_message.message)
        source_uuid = feedback_message.source_message_uuid.value
        if source_uuid not in expected_uuids:
            LOGGER.debug("Fetched message is for an unrecognised UUID. Discarding message.")
            return ParseResult(MessageStatus.DISCARD)  # We aren't interested in any messages for other UUIDs

        error_free = feedback_message.operation_was_error_free.value
        if not error_free:
            LOGGER.info(
                f"Cherrypicker test data create message with UUID '{source_uuid}' was not processed without " "errors."
            )
            return ParseResult(MessageStatus.HAS_ERRORS)

        return ParseResult(MessageStatus.VALID, source_uuid)
