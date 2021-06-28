from enum import Enum, auto


class ErrorLevel(Enum):
    DEBUG = 1
    INFO = 2
    WARNING = 3
    ERROR = 4
    CRITICAL = 5
    FATAL = 6


class CentreFileState(Enum):
    """An enum for file states."""

    FILE_UNCHECKED = auto()
    FILE_IN_BLACKLIST = auto()
    FILE_NOT_PROCESSED_YET = auto()
    FILE_PROCESSED_WITH_ERROR = auto()
    FILE_PROCESSED_WITH_SUCCESS = auto()
    FILE_SHOULD_NOT_BE_PROCESSED = auto()
