import crawler.helpers.general_helpers as general_helpers
from crawler.types import Config


class CPTDMessenger:
    def __init__(self, config: Config):
        self._config = config

        self.__schema_registry = None
        self.__basic_publisher = None

    @property
    def _schema_registry(self):
        if self.__schema_registry is None:
            self.__schema_registry = general_helpers.get_redpanda_schema_registry(self._config)

        return self.__schema_registry

    @property
    def _basic_publisher(self):
        if self.__basic_publisher is None:
            self.__basic_publisher = general_helpers.get_basic_publisher(self._config)

    def generate_test_data(create_plate_messages: list):
        pass
