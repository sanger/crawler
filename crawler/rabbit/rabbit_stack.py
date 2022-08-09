from lab_share_lib.config_readers import (
    get_basic_publisher,
    get_config,
    get_rabbit_server_details,
    get_redpanda_schema_registry,
)
from lab_share_lib.processing.rabbit_message_processor import RabbitMessageProcessor

from crawler.rabbit.background_consumer import BackgroundConsumer


class RabbitStack:
    def __init__(self, settings_module=""):
        config, settings_module = get_config(settings_module)

        rabbit_server_details = get_rabbit_server_details(config)
        schema_registry = get_redpanda_schema_registry(config)
        basic_publisher = get_basic_publisher(config)
        message_processor = RabbitMessageProcessor(schema_registry, basic_publisher, config)

        self._background_consumer = BackgroundConsumer(
            rabbit_server_details, config.RABBITMQ_CRUD_QUEUE, message_processor.process_message
        )

    @property
    def is_healthy(self):
        return self._background_consumer.is_healthy

    def bring_stack_up(self):
        if self._background_consumer.is_healthy:
            return

        self._background_consumer.start()
