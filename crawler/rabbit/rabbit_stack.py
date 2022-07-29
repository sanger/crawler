from crawler.helpers.general_helpers import get_config, get_rabbit_server_details
from crawler.processing.rabbit_message_processor import RabbitMessageProcessor
from crawler.rabbit.background_consumer import BackgroundConsumer
from crawler.rabbit.basic_publisher import BasicPublisher
from crawler.rabbit.schema_registry import SchemaRegistry


class RabbitStack:
    def __init__(self, settings_module=""):
        self._config, settings_module = get_config(settings_module)

        rabbit_crud_queue = self._config.RABBITMQ_CRUD_QUEUE
        self._background_consumer = BackgroundConsumer(
            get_rabbit_server_details(self._config), rabbit_crud_queue, self._rabbit_message_processor().process_message
        )

    @property
    def is_healthy(self):
        return self._background_consumer.is_healthy

    def _schema_registry(self):
        redpanda_url = self._config.REDPANDA_BASE_URI
        redpanda_api_key = self._config.REDPANDA_API_KEY
        return SchemaRegistry(redpanda_url, redpanda_api_key)

    def _rabbit_message_processor(self):
        basic_publisher = BasicPublisher(
            get_rabbit_server_details(self._config),
            self._config.RABBITMQ_PUBLISH_RETRY_DELAY,
            self._config.RABBITMQ_PUBLISH_RETRIES,
        )
        return RabbitMessageProcessor(self._schema_registry(), basic_publisher, self._config)

    def bring_stack_up(self):
        if self._background_consumer.is_healthy:
            return

        self._background_consumer.start()
