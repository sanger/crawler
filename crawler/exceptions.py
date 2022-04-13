class Error(Exception):
    """Base class for exceptions in this module."""

    pass


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


class DartStateError(Error):
    """Raised when the state of plate in DART is not a permitted value."""

    def __init__(self, message=None):
        self.message = message

    def __str__(self):
        default_message = "Error with the DART plate's state"

        if self.message:
            return f"DartStateError: {self.message}"
        else:
            return f"DartStateError: {default_message}"


class CherrypickerDataError(Error):
    """Raised during cherrypicker test data creation. The message is assumed to be user friendly."""

    def __init__(self, message):
        self.message = message


class RabbitProcessingError(Error):
    """Raised during intial processing of a RabbitMQ message, before the message has been deserialised."""

    def __init__(self, message, transient=False):
        """Constructs a new processing error message.

        Arguments:
            message {str} -- A message to log and possibly show to the user/caller.
            recoverable {bool} -- True if this error is transient, or
                                  False if the message will always cause this error.
        """
        self.message = message
        self.is_transient = transient
