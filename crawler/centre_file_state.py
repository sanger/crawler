from enum import Enum, auto


class CentreFileState(Enum):
    """An enum for file states."""

    FILE_UNCHECKED = auto()
    FILE_IN_BLACKLIST = auto()
    FILE_NOT_PROCESSED_YET = auto()
    FILE_PROCESSED_WITH_ERROR = auto()
    FILE_PROCESSED_WITH_SUCCESS = auto()
