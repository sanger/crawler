from crawler.constants import (
    RABBITMQ_HEADER_KEY_SUBJECT,
    RABBITMQ_HEADER_KEY_VERSION,
    RABBITMQ_SUBJECT_CREATE_PLATE_MAP,
    RABBITMQ_SUBJECT_CREATE_PLATE_MAP_FEEDBACK,
)
from crawler.rabbit.avro_encoder import AvroEncoder

MESSAGE_SUBJECTS = (RABBITMQ_SUBJECT_CREATE_PLATE_MAP, RABBITMQ_SUBJECT_CREATE_PLATE_MAP_FEEDBACK)


class RabbitMessageProcessor:
    def __init__(self, schema_registry, basic_publisher, config):
        self._encoders = {subject: AvroEncoder(schema_registry, subject) for subject in MESSAGE_SUBJECTS}
        self._basic_publisher = basic_publisher
        self._config = config

    def process_message(self, headers, body, acknowledge):
        try:
            subject = headers[RABBITMQ_HEADER_KEY_SUBJECT]
            version = headers[RABBITMQ_HEADER_KEY_VERSION]
            self._encoders[subject].decode(body, version)
            acknowledge(True)
        except ValueError:
            acknowledge(False)

    def publish_feedback_message(self, error_list):
        pass
