from crawler.exceptions import (
    Error,
    CentreFileError,
    DartStateError,
)


def test_error_is_type_of_exception():
    error = Error()
    assert isinstance(error, Exception)


def test_centre_file_error_is_type_of_error():
    error = CentreFileError()
    assert isinstance(error, Error)


def test_centre_file_error_reports_default_message():
    error = CentreFileError()
    assert error.__str__() is not None


def test_centre_file_error_reports_input_message():
    test_message = "This is a test message"
    error = CentreFileError(test_message)
    assert test_message in error.__str__()


def test_dart_state_error_is_type_of_error():
    error = DartStateError()
    assert isinstance(error, Error)


def test_dart_state_error_reports_default_message():
    error = DartStateError()
    assert error.__str__() is not None


def test_dart_state_error_reports_input_message():
    test_message = "This is a test message"
    error = DartStateError(test_message)
    assert test_message in error.__str__()
