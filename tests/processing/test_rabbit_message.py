from unittest.mock import MagicMock, Mock

import pytest

from crawler.constants import RABBITMQ_HEADER_KEY_SUBJECT, RABBITMQ_HEADER_KEY_VERSION
from crawler.processing.rabbit_message import RabbitMessage

HEADERS = {
    RABBITMQ_HEADER_KEY_SUBJECT: "a-subject",
    RABBITMQ_HEADER_KEY_VERSION: "3",
}

ENCODED_BODY = "Encoded body"
DECODED_LIST = ["Decoded body"]


@pytest.fixture
def subject():
    return RabbitMessage(HEADERS, ENCODED_BODY)


@pytest.fixture
def decoder():
    decoder = MagicMock()
    decoder.decode.return_value = DECODED_LIST

    return decoder


def test_subject_extracts_the_header_correctly(subject):
    assert subject.subject == HEADERS[RABBITMQ_HEADER_KEY_SUBJECT]


def test_schema_version_extracts_the_header_correctly(subject):
    assert subject.schema_version == HEADERS[RABBITMQ_HEADER_KEY_VERSION]


def test_decode_populates_decoded_list(subject, decoder):
    subject.decode(decoder)

    decoder.decode.assert_called_once_with(ENCODED_BODY, HEADERS[RABBITMQ_HEADER_KEY_VERSION])
    assert subject._decoded_list == DECODED_LIST


@pytest.mark.parametrize(
    "decoded_list,expected",
    [
        ([], False),
        (["decoded_1"], True),
        (["decoded_1", "decoded_2"], False),
    ],
)
def test_contains_single_message_gives_correct_response(subject, decoded_list, expected):
    subject._decoded_list = decoded_list
    assert subject.contains_single_message is expected


@pytest.mark.parametrize(
    "decoded_list,expected",
    [
        (["decoded_1"], "decoded_1"),
        # Realistically, you wouldn't be calling `.message` unless `.contains_single_message` returns True.  But anyway!
        (["decoded_1", "decoded_2"], "decoded_1"),
    ],
)
def test_message_returns_first_decoded_list_item(subject, decoded_list, expected):
    subject._decoded_list = decoded_list
    assert subject.message == expected


@pytest.mark.parametrize(
    "errors_to_add",
    [
        [],
        [Mock()],
        [Mock(), Mock()],
        [Mock(), Mock(), Mock()],
    ],
)
def test_add_error_appends_the_error_list(subject, errors_to_add):
    for error in errors_to_add:
        subject.add_error(error)

    assert subject.errors == errors_to_add


def test_initiate_count_sets_count_to_zero(subject):
    count_key = "a_count"
    subject.initiate_count(count_key)

    assert subject._counts[count_key] == 0


def test_increment_count_adds_one_to_count(subject):
    count_key = "a_count"
    subject._counts[count_key] = 5

    subject.increment_count(count_key)

    assert subject._counts[count_key] == 6


def test_get_count_gets_the_correct_count(subject):
    count_key = "a_count"
    subject._counts["first_count"] = 2
    subject._counts[count_key] = 4
    subject._counts["another_count"] = 6

    assert subject.get_count(count_key) == 4
