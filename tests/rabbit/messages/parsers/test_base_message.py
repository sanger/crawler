from unittest.mock import patch

import pytest

from crawler.rabbit.messages.parsers.base_message import BaseMessage


@pytest.fixture
def logger():
    with patch("crawler.rabbit.messages.base_message.LOGGER") as logger:
        yield logger


@pytest.fixture
def subject():
    return BaseMessage()


@pytest.mark.parametrize(
    "errors, headline",
    [
        [[], "No errors were reported during processing."],
        [["Error 1"], "1 error was reported during processing."],
        [["Error 1", "Error 2"], "2 errors were reported during processing."],
        [["Error 1", "Error 2", "Error 3"], "3 errors were reported during processing."],
        [["Error 1", "Error 2", "Error 3", "Error 4"], "4 errors were reported during processing."],
        [["Error 1", "Error 2", "Error 3", "Error 4", "Error 5"], "5 errors were reported during processing."],
    ],
)
def test_textual_errors_summary_is_accurate_for_up_to_5_errors(subject, errors, headline):
    subject._textual_errors = errors
    assert subject.textual_errors_summary == [headline] + errors


def test_textual_errors_summary_is_accurate_for_6_errors(subject):
    subject._textual_errors = ["Error 1", "Error 2", "Error 3", "Error 4", "Error 5", "Error 6"]
    assert subject.textual_errors_summary == [
        "6 errors were reported during processing. Only the first 5 are shown.",
        "Error 1",
        "Error 2",
        "Error 3",
        "Error 4",
        "Error 5",
    ]


@pytest.mark.parametrize(
    "textual_errors, error_count",
    [[[], 0], [["Error 1"], 1], [["Error 1", "Error 2"], 2], [["Error 1", "Error 2", "Error 3"], 3]],
)
def test_log_error_count_logs_error_count_correctly(subject, logger, textual_errors, error_count):
    subject._textual_errors = textual_errors

    subject.log_error_count()

    logger.debug.assert_called_once()
    log_message = logger.debug.call_args.args[0]
    assert str(error_count) in log_message
