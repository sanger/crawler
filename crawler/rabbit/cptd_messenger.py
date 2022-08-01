import crawler.helpers.general_helpers as general_helpers
from crawler.constants import (
    RABBITMQ_ROUTING_KEY_CREATE_PLATE,
    RABBITMQ_SUBJECT_CREATE_PLATE,
    RABBITMQ_SUBJECT_CREATE_PLATE_FEEDBACK,
)
from crawler.rabbit.avro_encoder import AvroEncoder
from crawler.rabbit.basic_publisher import BasicPublisher
from crawler.rabbit.messages.parsers.create_plate_message import FIELD_MESSAGE_UUID
from crawler.rabbit.schema_registry import SchemaRegistry
from crawler.types import Config


class CPTDMessenger:
    def __init__(self, config: Config):
        self._config = config

        self.__schema_registry = None
        self.__basic_publisher = None
        self.__encoder = None
        self.__decoder = None

    @property
    def _schema_registry(self) -> SchemaRegistry:
        if self.__schema_registry is None:
            self.__schema_registry = general_helpers.get_redpanda_schema_registry(self._config)

        return self.__schema_registry

    @property
    def _basic_publisher(self) -> BasicPublisher:
        if self.__basic_publisher is None:
            self.__basic_publisher = general_helpers.get_basic_publisher(
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

    def generate_test_data(self, create_plate_messages: list):
        """Send the plate messages to RabbitMQ then poll for the feedback messages indicating they were all processed
        correctly.  If there are any issues doing this, raises a CherrypickerDataError to populate the API response
        with.

        Arguments:
           create_plate_messages {list} -- a list of pre-prepared create plate messages to generate the test data with.
        """
        self._publish_messages(create_plate_messages)

        _ = [message[FIELD_MESSAGE_UUID] for message in create_plate_messages]

    def _publish_messages(self, create_plate_messages: list):
        for message in create_plate_messages:
            encoded_message = self._encoder.encode([message])
            self._basic_publisher.publish_message(
                self._config.RABBITMQ_CPTD_CRUD_EXCHANGE,
                RABBITMQ_ROUTING_KEY_CREATE_PLATE,
                encoded_message.body,
                RABBITMQ_SUBJECT_CREATE_PLATE,
                encoded_message.version,
            )
