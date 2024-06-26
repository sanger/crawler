from datetime import datetime
from types import ModuleType
from typing import Any, Dict, List, Optional, Tuple, TypedDict, Union

from bson.decimal128 import Decimal128
from bson.objectid import ObjectId
from typing_extensions import NotRequired

# Type aliases
CentreDoc = Dict[str, Any]  # mongo document that represents a centre
CSVRow = Dict[str, str]  # row of data from the CSV DictReader
ModifiedRowValue = Optional[Union[str, datetime, bool, int, Decimal128, ObjectId]]
ModifiedRow = Dict[str, ModifiedRowValue]
SampleDoc = Dict[str, ModifiedRowValue]  # mongo document that represents a sample
RowSignature = Tuple[str, ...]
SourcePlateDoc = Dict[str, Union[str, datetime]]  # mongo document that represents a source plate
DartWellProp = Dict[str, str]  # well properties of a DART well 'object'
FlaskResponse = Tuple[Dict[str, Any], int]  # a response from a Flask endpoint, including the status code


class CentreConf(TypedDict):
    backups_folder: NotRequired[str]
    barcode_field: NotRequired[str]
    barcode_regex: NotRequired[str]
    biomek_labware_class: str
    data_source: str
    feedback_routing_key_prefix: NotRequired[str]
    file_names_to_ignore: NotRequired[List[str]]
    include_in_scheduled_runs: NotRequired[bool]
    lab_id_default: str
    name: str
    prefix: NotRequired[str]
    sftp_file_regex_consolidated_eagle: NotRequired[str]
    sftp_file_regex_consolidated_surveillance: NotRequired[str]
    sftp_file_regex_unconsolidated_surveillance: NotRequired[str]
    sftp_root_read: NotRequired[str]
    skip_unconsolidated_surveillance_files: NotRequired[bool]


class Config(ModuleType):
    """ModuleType class for the app config."""

    CENTRES: List[CentreConf]
    LOGGING: Dict[str, Any]

    # General
    ADD_LAB_ID: bool
    DIR_DOWNLOADED_DATA: str

    # Cherrypicker Test Data
    ENABLE_CHERRYPICKER_ENDPOINTS: bool
    MAX_PLATES_PER_TEST_DATA_RUN: int

    CPTD_FEEDBACK_WAIT_TIME: int

    # Ingest Behaviour
    USE_SFTP: bool
    KEEP_FILES: bool
    ADD_TO_DART: bool

    # Baracoda
    BARACODA_BASE_URL: str
    BARACODA_RETRY_ATTEMPTS: int

    # Mongo
    MONGO_DB: str
    MONGO_URI: str

    # MLWH
    MLWH_DB_HOST: str
    MLWH_DB_PORT: int
    MLWH_DB_RO_USER: str
    MLWH_DB_RO_PASSWORD: str
    MLWH_DB_RW_USER: str
    MLWH_DB_RW_PASSWORD: str
    MLWH_DB_DBNAME: str
    EVENTS_WH_DB: str

    # DART
    DART_DB_HOST: str
    DART_DB_PORT: int
    DART_DB_RW_USER: str
    DART_DB_RW_PASSWORD: str
    DART_DB_DBNAME: str
    DART_DB_DRIVER: str

    # RabbitMQ
    RABBITMQ_HOST: str
    RABBITMQ_SSL: bool
    RABBITMQ_PORT: int
    RABBITMQ_USERNAME: str
    RABBITMQ_PASSWORD: str
    RABBITMQ_VHOST: str
    RABBITMQ_CRUD_QUEUE: str
    RABBITMQ_FEEDBACK_EXCHANGE: str

    RABBITMQ_CPTD_USERNAME: str
    RABBITMQ_CPTD_PASSWORD: str
    RABBITMQ_CPTD_CRUD_EXCHANGE: str
    RABBITMQ_CPTD_FEEDBACK_QUEUE: str

    RABBITMQ_PUBLISH_RETRY_DELAY: int
    RABBITMQ_PUBLISH_RETRIES: int

    # RedPanda
    REDPANDA_BASE_URI: str

    # SFTP
    SFTP_HOST: str
    SFTP_PORT: int
    SFTP_READ_USERNAME: str
    SFTP_READ_PASSWORD: str

    # APScheduler
    SCHEDULER_RUN: bool
    SCHEDULER_TIMEZONE: str
    SCHEDULER_API_ENABLED: bool
    JOBS: List[Dict[str, str]]


class RabbitServerDetails(ModuleType):
    """ModuleType class for details to connect to a RabbitMQ server."""

    uses_ssl: bool
    host: str
    port: int
    username: str
    password: str
    vhost: str

    def __init__(self, uses_ssl, host, port, username, password, vhost):
        self.uses_ssl = uses_ssl
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.vhost = vhost or "/"
