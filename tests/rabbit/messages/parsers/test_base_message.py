import pytest

from crawler.rabbit.messages.parsers.base_message import BaseMessage


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
