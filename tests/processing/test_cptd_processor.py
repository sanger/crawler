from unittest.mock import MagicMock, call, patch

import pytest

from crawler.exceptions import CherrypickerDataError
from crawler.processing.cptd_processor import CPTDProcessor
from crawler.rabbit.basic_getter import FetchedMessage

CREATE_PLATE_MESSAGES = [{"messageUuid": b"UUID_ONE"}, {"messageUuid": b"UUID_TWO"}]

ERROR_FREE_FEEDBACK_BODIES = [
    [{"sourceMessageUuid": message["messageUuid"].decode(), "operationWasErrorFree": True}]
    for message in CREATE_PLATE_MESSAGES
]
ERROR_FREE_FEEDBACK = [
    FetchedMessage(headers={"subject": "create-plate-map-feedback", "version": 4}, body=body)
    for body in ERROR_FREE_FEEDBACK_BODIES
]

WRONG_SUBJECT_BODY = ERROR_FREE_FEEDBACK_BODIES[0]
WRONG_SUBJECT_FEEDBACK = [FetchedMessage(headers={"subject": "wrong-subject", "version": 4}, body=WRONG_SUBJECT_BODY)]

MULTIPLE_MESSAGE_BODY = [
    {"sourceMessageUuid": message["messageUuid"].decode(), "operationWasErrorFree": True}
    for message in CREATE_PLATE_MESSAGES
]
MULTIPLE_MESSAGE_FEEDBACK = [
    FetchedMessage(headers={"subject": "create-plate-map-feedback", "version": 4}, body=MULTIPLE_MESSAGE_BODY)
]

UNRECOGNISED_UUID_BODY = [{"sourceMessageUuid": "UUID_THREE", "operationWasErrorFree": True}]
UNRECOGNISED_UUID_FEEDBACK = [
    FetchedMessage(headers={"subject": "create-plate-map-feedback", "version": 4}, body=UNRECOGNISED_UUID_BODY)
]

ERRORED_FEEDBACK_BODIES = [
    [{"sourceMessageUuid": message["messageUuid"].decode(), "operationWasErrorFree": False}]
    for message in CREATE_PLATE_MESSAGES
]
ERRORED_FEEDBACK = [
    FetchedMessage(headers={"subject": "create-plate-map-feedback", "version": 4}, body=body)
    for body in ERRORED_FEEDBACK_BODIES
]


@pytest.fixture
def logger():
    with patch("crawler.processing.cptd_processor.LOGGER") as mock:
        yield mock


@pytest.fixture(autouse=True)
def avro_encoder():
    with patch("crawler.processing.cptd_processor.AvroEncoder") as mock:
        yield mock.return_value


@pytest.fixture(autouse=True)
def get_rabbit_server_details():
    with patch("crawler.helpers.general_helpers.get_rabbit_server_details") as mock:
        yield mock


@pytest.fixture(autouse=True)
def get_basic_publisher():
    with patch("crawler.helpers.general_helpers.get_basic_publisher") as mock:
        yield mock


@pytest.fixture(autouse=True)
def basic_getter():
    with patch("crawler.processing.cptd_processor.BasicGetter") as mock:
        yield mock.return_value.__enter__.return_value


@pytest.fixture
def subject(config):
    return CPTDProcessor(config)


def test_constructor_stores_config(subject, config):
    assert subject._config == config


def test_generate_test_data_logs_stages_of_process(subject, logger):
    subject.generate_test_data([])

    assert logger.info.call_count == 4
    assert "Starting" in logger.info.call_args_list[0].args[0]
    assert "Publishing" in logger.info.call_args_list[1].args[0]
    assert "Beginning loop" in logger.info.call_args_list[2].args[0]
    assert "completed successfully" in logger.info.call_args_list[3].args[0]


def test_generate_test_data_when_error_free_feedback(
    subject, config, logger, get_basic_publisher, get_rabbit_server_details, avro_encoder, basic_getter
):
    avro_encoder.decode.side_effect = ERROR_FREE_FEEDBACK_BODIES
    basic_getter.get_message.side_effect = ERROR_FREE_FEEDBACK

    subject.generate_test_data(CREATE_PLATE_MESSAGES)

    logger.error.assert_not_called()
    logger.debug.assert_not_called()
    assert "completed successfully" in logger.info.call_args.args[0]

    get_basic_publisher.assert_called_once_with(config, config.RABBITMQ_CPTD_USERNAME, config.RABBITMQ_CPTD_PASSWORD)
    avro_encoder.encode.assert_has_calls([call([CREATE_PLATE_MESSAGES[0]]), call([CREATE_PLATE_MESSAGES[1]])])
    get_rabbit_server_details.assert_called_once_with(
        config, config.RABBITMQ_CPTD_USERNAME, config.RABBITMQ_CPTD_PASSWORD
    )
    basic_getter.get_message.assert_has_calls(
        [call(config.RABBITMQ_CPTD_FEEDBACK_QUEUE), call(config.RABBITMQ_CPTD_FEEDBACK_QUEUE)]
    )
    avro_encoder.decode.assert_has_calls(
        [call(ERROR_FREE_FEEDBACK_BODIES[0], 4), call(ERROR_FREE_FEEDBACK_BODIES[1], 4)]
    )


def test_generate_test_data_when_feedback_in_wrong_order(subject, logger, avro_encoder, basic_getter):
    avro_encoder.decode.side_effect = ERROR_FREE_FEEDBACK_BODIES[::-1]  # reverse the messages
    basic_getter.get_message.side_effect = ERROR_FREE_FEEDBACK[::-1]  # reverse the messages

    subject.generate_test_data(CREATE_PLATE_MESSAGES)

    logger.error.assert_not_called()
    logger.debug.assert_not_called()
    assert "completed successfully" in logger.info.call_args.args[0]


def test_generate_test_data_when_no_feedback(subject, logger, basic_getter):
    basic_getter.get_message.side_effect = [None] * 10

    with pytest.raises(CherrypickerDataError) as ex_info:
        subject.generate_test_data(CREATE_PLATE_MESSAGES)

    assert "failed" in str(ex_info.value)
    logger.error.assert_called_once()
    assert "completed successfully" not in logger.info.call_args.args[0]


def test_generate_test_data_when_partial_feedback(subject, logger, avro_encoder, basic_getter):
    avro_encoder.decode.side_effect = ERROR_FREE_FEEDBACK_BODIES
    basic_getter.get_message.side_effect = [ERROR_FREE_FEEDBACK[1]] + [None] * 10

    with pytest.raises(CherrypickerDataError) as ex_info:
        subject.generate_test_data(CREATE_PLATE_MESSAGES)

    assert "failed" in str(ex_info.value)
    logger.error.assert_called_once()
    assert "completed successfully" not in logger.info.call_args.args[0]


def test_generate_test_data_when_queue_is_empty_to_start_with(subject, logger, avro_encoder, basic_getter):
    avro_encoder.decode.side_effect = ERROR_FREE_FEEDBACK_BODIES  # We don't decode the missing message
    basic_getter.get_message.side_effect = [None] + ERROR_FREE_FEEDBACK

    subject.generate_test_data(CREATE_PLATE_MESSAGES)

    logger.error.assert_not_called()
    logger.debug.assert_not_called()
    assert "completed successfully" in logger.info.call_args.args[0]


def test_generate_test_data_when_message_with_wrong_subject_starts_queue(subject, logger, avro_encoder, basic_getter):
    avro_encoder.decode.side_effect = ERROR_FREE_FEEDBACK_BODIES  # We don't decode the wrong subject message
    basic_getter.get_message.side_effect = WRONG_SUBJECT_FEEDBACK + ERROR_FREE_FEEDBACK

    subject.generate_test_data(CREATE_PLATE_MESSAGES)

    logger.error.assert_not_called()
    logger.debug.assert_called_once()
    assert "'wrong-subject'" in logger.debug.call_args.args[0]
    assert "completed successfully" in logger.info.call_args.args[0]


def test_generate_test_data_when_message_with_multiple_messages_starts_queue(
    subject, logger, avro_encoder, basic_getter
):
    avro_encoder.decode.side_effect = [MULTIPLE_MESSAGE_BODY] + ERROR_FREE_FEEDBACK_BODIES
    basic_getter.get_message.side_effect = MULTIPLE_MESSAGE_FEEDBACK + ERROR_FREE_FEEDBACK

    subject.generate_test_data(CREATE_PLATE_MESSAGES)

    logger.error.assert_not_called()
    logger.debug.assert_called_once()
    assert "contains more than one" in logger.debug.call_args.args[0]
    assert "completed successfully" in logger.info.call_args.args[0]


def test_generate_test_data_when_message_with_unrecognised_uuid_starts_queue(
    subject, logger, avro_encoder, basic_getter
):
    avro_encoder.decode.side_effect = [UNRECOGNISED_UUID_BODY] + ERROR_FREE_FEEDBACK_BODIES
    basic_getter.get_message.side_effect = UNRECOGNISED_UUID_FEEDBACK + ERROR_FREE_FEEDBACK

    subject.generate_test_data(CREATE_PLATE_MESSAGES)

    logger.error.assert_not_called()
    logger.debug.assert_called_once()
    assert "unrecognised UUID" in logger.debug.call_args.args[0]
    assert "completed successfully" in logger.info.call_args.args[0]


def test_generate_test_data_when_processing_generated_errors(subject, logger, avro_encoder, basic_getter):
    avro_encoder.decode.side_effect = ERRORED_FEEDBACK_BODIES
    basic_getter.get_message.side_effect = ERRORED_FEEDBACK

    with pytest.raises(CherrypickerDataError) as ex_info:
        subject.generate_test_data(CREATE_PLATE_MESSAGES)

    assert "failed" in str(ex_info.value)

    logger.error.assert_called_once()
    logger.debug.assert_not_called()

    basic_getter.get_message.assert_called_once()  # The second message wasn't read after the initial failed message.

    assert "'UUID_ONE'" in logger.info.call_args.args[0]
    assert "not processed without errors" in logger.info.call_args.args[0]
