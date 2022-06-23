import copy
from datetime import datetime, timedelta
from http import HTTPStatus
from unittest.mock import ANY, MagicMock, patch

import pytest
import responses

from crawler.constants import (
    COLLECTION_SAMPLES,
    DART_STATE_NO_PLATE,
    DART_STATE_PENDING,
    DART_STATE_PICKABLE,
    FIELD_LH_SAMPLE_UUID,
    FIELD_MUST_SEQUENCE,
    FIELD_PLATE_BARCODE,
    FIELD_PREFERENTIALLY_SEQUENCE,
    FIELD_UPDATED_AT,
)
from crawler.db.mongo import get_mongo_collection
from crawler.exceptions import TransientRabbitError
from crawler.processing.update_sample_exporter import UpdateSampleExporter
from crawler.rabbit.messages.update_sample_message import (
    FIELD_SAMPLE,
    FIELD_UPDATED_FIELDS,
    ErrorType,
    UpdateSampleMessage,
)
from tests.testing_objects import UPDATE_SAMPLE_MESSAGE


@pytest.fixture
def logger():
    with patch("crawler.processing.update_sample_exporter.LOGGER") as logger:
        yield logger


@pytest.fixture
def update_sample_message():
    return UpdateSampleMessage(copy.deepcopy(UPDATE_SAMPLE_MESSAGE))


@pytest.fixture
def dart_connection():
    with patch("crawler.processing.update_sample_exporter.create_dart_sql_server_conn") as connection:
        yield connection


@pytest.fixture
def subject(update_sample_message, config, dart_connection):
    return UpdateSampleExporter(update_sample_message, config)


#####
# Helper methods
#####


def dummy_mongo_sample(updated_at=None):
    if updated_at is None:
        updated_at = datetime.now() - timedelta(hours=1)

    return {
        FIELD_LH_SAMPLE_UUID: "UPDATE_SAMPLE_UUID",
        FIELD_UPDATED_AT: updated_at,
        FIELD_PLATE_BARCODE: "A_PLATE_BARCODE",
    }


def add_sample_to_mongo(mongo_database, updated_at=None):
    _, mongo_database = mongo_database
    samples_collection = get_mongo_collection(mongo_database, COLLECTION_SAMPLES)
    samples_collection.insert_one(dummy_mongo_sample(updated_at))


def add_sample_to_subject(subject):
    subject._mongo_sample = dummy_mongo_sample()


def set_up_response_for_cherrytrack_plate(subject, config, http_status):
    add_sample_to_subject(subject)
    cherrytrack_url = f"{config.CHERRYTRACK_BASE_URL}/source-plates/A_PLATE_BARCODE"
    responses.add(responses.GET, cherrytrack_url, status=http_status)


#####
# End of helper methods
#####


def test_constructor_stores_arguments_as_instance_variables():
    message = MagicMock()
    config = MagicMock()
    subject = UpdateSampleExporter(message, config)

    assert subject._message == message
    assert subject._config == config


def test_mongo_db_gets_the_mongo_instance(subject, mongo_database):
    _, mongo_database = mongo_database

    assert subject._mongo_db == mongo_database


def test_verify_sample_in_mongo_when_no_sample_in_mongo(subject):
    subject.verify_sample_in_mongo()

    assert len(subject._message.feedback_errors) == 1
    assert subject._message.feedback_errors[0]["typeId"] == ErrorType.ExporterSampleDoesNotExist
    assert subject._mongo_sample is None


def test_verify_sample_in_mongo_when_sample_is_present(subject, mongo_database):
    add_sample_to_mongo(mongo_database)

    subject.verify_sample_in_mongo()

    assert len(subject._message.feedback_errors) == 0
    assert subject._plate_barcode == "A_PLATE_BARCODE"


def test_verify_sample_in_mongo_when_sample_is_more_recently_updated_than_the_message(subject, mongo_database):
    add_sample_to_mongo(mongo_database, datetime.now() + timedelta(hours=1))

    subject.verify_sample_in_mongo()

    assert len(subject._message.feedback_errors) == 1
    assert subject._message.feedback_errors[0]["typeId"] == ErrorType.ExporterMessageOutOfDate
    assert subject._mongo_sample is None


def test_verify_sample_in_mongo_when_mongo_raises_an_exception(subject, logger):
    timeout_error = TimeoutError()

    with patch("crawler.processing.update_sample_exporter.get_mongo_collection") as get_collection:
        get_collection.side_effect = timeout_error

        with pytest.raises(TransientRabbitError) as ex_info:
            subject.verify_sample_in_mongo()

    logger.critical.assert_called_once()
    assert "'UPDATE_SAMPLE_UUID'" in logger.critical.call_args.args[0]

    logger.exception.assert_called_once_with(timeout_error)

    assert "'UPDATE_SAMPLE_UUID'" in ex_info.value.message

    assert subject._mongo_sample is None


def test_verify_plate_state_raises_value_error_when_plate_barcode_not_set(subject):
    with pytest.raises(ValueError) as ex_info:
        subject.verify_plate_state()

    assert "Mongo sample" in str(ex_info.value)


@responses.activate
def test_verify_plate_state_adds_message_error_when_plate_in_cherrytrack(subject, config):
    set_up_response_for_cherrytrack_plate(subject, config, HTTPStatus.OK)

    subject.verify_plate_state()

    assert len(subject._message.feedback_errors) == 1
    assert subject._message.feedback_errors[0]["typeId"] == ErrorType.ExporterPlateAlreadyPicked


@responses.activate
def test_verify_plate_state_raises_transient_error_when_cherrytrack_is_not_responding(subject, logger):
    add_sample_to_subject(subject)
    # Don't add the cherrytrack endpoint to responses

    with pytest.raises(TransientRabbitError) as ex_info:
        subject.verify_plate_state()

    logger.exception.assert_called_once()
    assert "'A_PLATE_BARCODE'" in ex_info.value.message


@responses.activate
def test_verify_plate_state_passes_message_when_plate_is_pending_in_dart(subject, config, dart_connection):
    set_up_response_for_cherrytrack_plate(subject, config, HTTPStatus.NOT_FOUND)

    with patch("crawler.processing.update_sample_exporter.get_dart_plate_state") as get_plate_state:
        get_plate_state.return_value = DART_STATE_PENDING
        subject.verify_plate_state()

    assert len(subject._message.feedback_errors) == 0
    dart_connection.return_value.close.assert_called_once()


@responses.activate
def test_verify_plate_state_adds_message_error_when_plate_is_pickable_in_dart(subject, config, dart_connection):
    set_up_response_for_cherrytrack_plate(subject, config, HTTPStatus.NOT_FOUND)

    with patch("crawler.processing.update_sample_exporter.get_dart_plate_state") as get_plate_state:
        get_plate_state.return_value = DART_STATE_PICKABLE
        subject.verify_plate_state()

    assert len(subject._message.feedback_errors) == 1
    assert subject._message.feedback_errors[0]["typeId"] == ErrorType.ExporterPlateAlreadyPicked
    dart_connection.return_value.close.assert_called_once()


@responses.activate
def test_verify_plate_state_logs_issue_but_passes_plate_when_plate_is_missing_in_dart(
    subject, config, logger, dart_connection
):
    set_up_response_for_cherrytrack_plate(subject, config, HTTPStatus.NOT_FOUND)

    with patch("crawler.processing.update_sample_exporter.get_dart_plate_state") as get_plate_state:
        get_plate_state.return_value = DART_STATE_NO_PLATE
        subject.verify_plate_state()

    assert len(subject._message.feedback_errors) == 0
    logger.critical.assert_called_once()
    assert "the plate does not exist" in logger.critical.call_args.args[0]
    dart_connection.return_value.close.assert_called_once()


@responses.activate
def test_verify_plate_state_raises_transient_error_when_dart_connection_cannot_be_made(
    subject, config, dart_connection
):
    set_up_response_for_cherrytrack_plate(subject, config, HTTPStatus.NOT_FOUND)
    dart_connection.return_value = None

    with pytest.raises(TransientRabbitError) as ex_info:
        subject.verify_plate_state()

    assert "connecting to the DART database" in ex_info.value.message
    assert "'A_PLATE_BARCODE'" in ex_info.value.message


@responses.activate
def test_verify_plate_state_raises_transient_error_when_dart_query_cannot_be_made(
    subject, config, logger, dart_connection
):
    set_up_response_for_cherrytrack_plate(subject, config, HTTPStatus.NOT_FOUND)

    with patch("crawler.processing.update_sample_exporter.get_dart_plate_state") as get_plate_state:
        get_plate_state.side_effect = Exception("Boom!")
        with pytest.raises(TransientRabbitError) as ex_info:
            subject.verify_plate_state()

    logger.exception.assert_called_once()
    assert "querying the DART database" in ex_info.value.message
    assert "'A_PLATE_BARCODE'" in ex_info.value.message
    dart_connection.return_value.close.assert_called_once()


@pytest.mark.parametrize(
    "fields, must_sequence, preferentially_sequence",
    [
        [{"mustSequence": True}, True, None],
        [{"mustSequence": False}, False, None],
        [{"preferentiallySequence": True}, None, True],
        [{"preferentiallySequence": False}, None, False],
        [{"mustSequence": True, "preferentiallySequence": True}, True, True],
        [{"mustSequence": False, "preferentiallySequence": False}, False, False],
    ],
)
def test_update_mongo_updates_the_sample(subject, mongo_database, fields, must_sequence, preferentially_sequence):
    subject._message._body[FIELD_SAMPLE][FIELD_UPDATED_FIELDS] = [
        {"name": name, "value": value} for name, value in fields.items()
    ]

    add_sample_to_mongo(mongo_database)
    add_sample_to_subject(subject)
    _, mongo_database = mongo_database

    subject.update_mongo()

    samples_collection = get_mongo_collection(mongo_database, COLLECTION_SAMPLES)
    sample = samples_collection.find_one({}) or {}

    if must_sequence is None:
        assert FIELD_MUST_SEQUENCE not in sample.keys()
    else:
        assert sample[FIELD_MUST_SEQUENCE] == must_sequence

    if preferentially_sequence is None:
        assert FIELD_PREFERENTIALLY_SEQUENCE not in sample.keys()
    else:
        assert sample[FIELD_PREFERENTIALLY_SEQUENCE] == preferentially_sequence


def test_update_mongo_logs_info(subject, logger):
    add_sample_to_subject(subject)

    subject.update_mongo()

    logger.info.assert_called_once()
    assert "Updating" in logger.info.call_args.args[0]


def test_update_mongo_when_connection_fails(subject, logger):
    exception = Exception("Boom!")

    with patch("crawler.processing.update_sample_exporter.get_mongo_collection") as get_mongo_collection:
        get_mongo_collection.side_effect = exception

        with pytest.raises(TransientRabbitError) as ex_info:
            subject.update_mongo()

    logger.critical.assert_called_once()
    assert "'UPDATE_SAMPLE_UUID'" in logger.critical.call_args.args[0]

    logger.exception.assert_called_once_with(exception)

    assert "'UPDATE_SAMPLE_UUID'" in ex_info.value.message


def test_update_dart_connects_to_the_database(subject, dart_connection):
    subject.update_dart()

    dart_connection.assert_called()


def test_update_dart_logs_if_dart_connection_fails(subject, dart_connection, logger):
    add_sample_to_subject(subject)
    dart_connection.return_value = None

    subject.update_dart()

    logger.critical.assert_called_once()
    assert "'UPDATE_SAMPLE_UUID'" in logger.critical.call_args.args[0]

    assert len(subject._message.feedback_errors) == 1
    assert subject._message.feedback_errors[0]["typeId"] == ErrorType.ExporterDARTUpdateFailed


@pytest.mark.parametrize(
    "fields, must_sequence, preferentially_sequence",
    [
        [{"mustSequence": True}, True, None],
        [{"mustSequence": False}, False, None],
        [{"preferentiallySequence": True}, None, True],
        [{"preferentiallySequence": False}, None, False],
        [{"mustSequence": True, "preferentiallySequence": True}, True, True],
        [{"mustSequence": False, "preferentiallySequence": False}, False, False],
    ],
)
def test_update_dart_updates_well_properties_with_correct_values(
    subject, dart_connection, fields, must_sequence, preferentially_sequence
):
    subject._message._body[FIELD_SAMPLE][FIELD_UPDATED_FIELDS] = [
        {"name": name, "value": value} for name, value in fields.items()
    ]

    add_sample_to_subject(subject)

    with patch("crawler.processing.update_sample_exporter.add_dart_well_properties_if_positive") as add_method:
        subject.update_dart()

    assert len(subject._message.feedback_errors) == 0

    add_method.assert_called_once_with(dart_connection.return_value.cursor(), ANY, "A_PLATE_BARCODE")
    updated_sample = add_method.call_args.args[1]

    if must_sequence is None:
        assert FIELD_MUST_SEQUENCE not in updated_sample.keys()
    else:
        assert updated_sample[FIELD_MUST_SEQUENCE] == must_sequence

    if preferentially_sequence is None:
        assert FIELD_PREFERENTIALLY_SEQUENCE not in updated_sample.keys()
    else:
        assert updated_sample[FIELD_PREFERENTIALLY_SEQUENCE] == preferentially_sequence


def test_update_dart_logs_if_update_well_properties_for_dart_fails(subject, logger):
    add_sample_to_subject(subject)
    add_exception = Exception("Boom!")

    with patch("crawler.processing.update_sample_exporter.add_dart_well_properties_if_positive") as add_method:
        add_method.side_effect = add_exception
        subject.update_dart()

    logger.exception.assert_called_once_with(add_exception)

    logger.critical.assert_called_once()
    assert "'UPDATE_SAMPLE_UUID'" in logger.critical.call_args.args[0]

    assert len(subject._message.feedback_errors) == 1
    assert subject._message.feedback_errors[0]["typeId"] == ErrorType.ExporterDARTUpdateFailed
