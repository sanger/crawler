from crawler.helpers.general_helpers import get_config
from crawler.processing.rabbit_message_processor import RabbitMessageProcessor
from crawler.rabbit.background_consumer import BackgroundConsumer
from crawler.rabbit.basic_publisher import BasicPublisher
from crawler.rabbit.schema_registry import SchemaRegistry
from crawler.types import RabbitServerDetails


class RabbitStack:
    def __init__(self, settings_module=""):
        self._config, settings_module = get_config(settings_module)

    def _rabbit_server_details(self):
        return RabbitServerDetails(
            uses_ssl=self._config.RABBITMQ_SSL,
            host=self._config.RABBITMQ_HOST,
            port=self._config.RABBITMQ_PORT,
            username=self._config.RABBITMQ_USERNAME,
            password=self._config.RABBITMQ_PASSWORD,
            vhost=self._config.RABBITMQ_VHOST,
        )

    def _schema_registry(self):
        redpanda_url = self._config.REDPANDA_BASE_URI
        redpanda_api_key = self._config.REDPANDA_API_KEY
        return SchemaRegistry(redpanda_url, redpanda_api_key)

    def _rabbit_message_processor(self):
        basic_publisher = BasicPublisher(self._rabbit_server_details())
        return RabbitMessageProcessor(self._schema_registry(), basic_publisher, self._config)

    def bring_stack_up(self):
        rabbit_crud_queue = self._config.RABBITMQ_CRUD_QUEUE
        BackgroundConsumer(
            self._rabbit_server_details(), rabbit_crud_queue, self._rabbit_message_processor().process_message
        ).start()
