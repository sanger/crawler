from datetime import datetime
from types import ModuleType
from typing import Any, Dict, List, Optional, Tuple, Union

from bson.decimal128 import Decimal128

# Type aliases
CentreDoc = Dict[str, Any]  #  mongo document that represents a centre
CSVRow = Dict[str, str]  # row of data from the CSV DictReader
ModifiedRowValue = Optional[Union[str, datetime, bool, int, Decimal128]]
ModifiedRow = Dict[str, ModifiedRowValue]
SampleDoc = Dict[str, ModifiedRowValue]  # mongo document that represents a sample
SamplePriorityDoc = Dict[str, ModifiedRowValue]  # mongo document that represents a sample priority
RowSignature = Tuple[str, ...]
CentreConf = Dict[str, str]  # config for a centre
SourcePlateDoc = Dict[str, Union[str, datetime]]  # mongo document that represents a source plate
DartWellProp = Dict[str, str]  # well properties of a DART well 'object'


class Config(ModuleType):
    """ModuleType class for the app config."""

    CENTRES: List[CentreConf]
    LOGGING: Dict[str, Any]

    # General
    ADD_LAB_ID: bool
    DIR_DOWNLOADED_DATA: str

    # Mongo
    MONGO_URI: str
    MONGO_HOST: str
    MONGO_PORT: int
    MONGO_USERNAME: str
    MONGO_PASSWORD: str
    MONGO_DB: str

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

    # SFTP
    SFTP_HOST: str
    SFTP_PORT: int
    SFTP_READ_USERNAME: str
    SFTP_READ_PASSWORD: str
