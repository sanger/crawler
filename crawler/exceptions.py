class Error(Exception):
    """Base class for exceptions in this module."""

    pass


class RequiredConfigError(Error):
    """Raised when required config is not present."""

    def __init__(self, message=None):
        self.message = message

    def __str__(self):
        default_message = "Required config is missing"

        if self.message:
            return f"RequiredConfigError: {self.message}"
        else:
            return f"RequiredConfigError: {default_message}"


class CentreFileError(Error):
    """Raised when there is an error with the centre's CSV file."""

    def __init__(self, message=None):
        self.message = message

    def __str__(self):
        default_message = "Error with the centre's CSV file"

        if self.message:
            return f"CentreFileError: {self.message}"
        else:
            return f"CentreFileError: {default_message}"
