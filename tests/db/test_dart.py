from unittest.mock import patch

import pyodbc
import pytest

from crawler.constants import (
    DART_STATE,
    DART_STATE_NO_PLATE,
    DART_STATE_NO_PROP,
    DART_STATE_PENDING,
    FIELD_RESULT,
    RESULT_VALUE_POSITIVE,
)
from crawler.db.dart import (
    add_dart_plate_if_doesnt_exist,
    create_dart_sql_server_conn,
    get_dart_plate_state,
    set_dart_plate_state_pending,
    set_dart_well_properties,
    add_dart_well_properties_if_positive,
)
from crawler.exceptions import DartStateError
from crawler.sql_queries import (
    SQL_DART_ADD_PLATE,
    SQL_DART_GET_PLATE_PROPERTY,
    SQL_DART_SET_PLATE_PROPERTY,
    SQL_DART_SET_WELL_PROPERTY,
)

from tests.conftest import generate_new_object_for_string


def test_add_dart_well_properties_if_positive(mlwh_connection):
    with patch("crawler.db.dart.add_dart_well_properties") as mock_add_dart_well_properties:
        cursor = mlwh_connection.cursor(dictionary=True)
        sample = {FIELD_RESULT: generate_new_object_for_string(RESULT_VALUE_POSITIVE)}
        plate_barcode = "aBarcode"
        add_dart_well_properties_if_positive(cursor, sample, plate_barcode)
        mock_add_dart_well_properties.assert_called_with(cursor, sample, plate_barcode)


def test_create_dart_sql_server_conn(config):
    with patch("pyodbc.connect") as mock_connect:
        conn_string = (
            f"DRIVER={config.DART_DB_DRIVER};SERVER={config.DART_DB_HOST};"
            f"PORT={config.DART_DB_PORT};DATABASE={config.DART_DB_DBNAME};"
            f"UID={config.DART_DB_RW_USER};PWD={config.DART_DB_RW_PASSWORD}"
        )
        create_dart_sql_server_conn(config)
        mock_connect.assert_called_with(conn_string)


def test_create_dart_sql_server_conn_none(config):
    with patch("pyodbc.connect", return_value=None):
        assert create_dart_sql_server_conn(config) is None


def test_create_dart_sql_server_conn_expection(config):
    with patch("pyodbc.connect", side_effect=pyodbc.Error()):
        assert create_dart_sql_server_conn(config) is None


def test_get_dart_plate_state(config):
    with patch("pyodbc.connect") as mock_conn:
        test_plate_barcode = "AB123"
        assert get_dart_plate_state(mock_conn.cursor(), test_plate_barcode) == str(mock_conn.cursor().fetchval())
        mock_conn.cursor().execute.assert_called_with(SQL_DART_GET_PLATE_PROPERTY, (test_plate_barcode, DART_STATE))


def test_set_dart_plate_state_pending(config):
    with patch("pyodbc.connect") as mock_conn:
        test_plate_barcode = "AB123"
        set_dart_plate_state_pending(mock_conn.cursor(), test_plate_barcode)
        mock_conn.cursor().execute.assert_called_with(
            SQL_DART_SET_PLATE_PROPERTY,
            (test_plate_barcode, DART_STATE, DART_STATE_PENDING),
        )


def test_set_dart_well_properties(config):
    with patch("pyodbc.connect") as mock_conn:
        test_plate_barcode = "AB123"
        test_well_props = {"prop1": "value1", "test prop": "test value"}
        test_well_index = 12
        set_dart_well_properties(mock_conn.cursor(), test_plate_barcode, test_well_props, test_well_index)
        for prop_name, prop_value in test_well_props.items():
            mock_conn.cursor().execute.assert_any_call(
                SQL_DART_SET_WELL_PROPERTY,
                (test_plate_barcode, prop_name, prop_value, test_well_index),
            )


def test_add_dart_plate_if_doesnt_exist_throws_without_state_property(config):
    with patch("pyodbc.connect") as mock_conn:
        test_plate_barcode = "AB123"
        test_labclass = "test class"

        # does not create existing plate and returns its state
        with patch("crawler.db.dart.get_dart_plate_state", return_value=DART_STATE_PENDING):
            result = add_dart_plate_if_doesnt_exist(mock_conn.cursor(), test_plate_barcode, test_labclass)
            mock_conn.cursor().assert_not_called()
            assert result == DART_STATE_PENDING

        # if plate does not exist, creates new plate with pending state
        with patch("crawler.db.dart.get_dart_plate_state", return_value=DART_STATE_NO_PLATE):
            with patch("crawler.db.dart.set_dart_plate_state_pending", return_value=True):
                result = add_dart_plate_if_doesnt_exist(mock_conn.cursor(), test_plate_barcode, test_labclass)
                mock_conn.cursor().execute.assert_called_with(
                    SQL_DART_ADD_PLATE, (test_plate_barcode, test_labclass, 96)
                )
                assert result == DART_STATE_PENDING

        # if plate does not exist, throws on failure adding setting new plate state to pending
        with patch("crawler.db.dart.get_dart_plate_state", return_value=DART_STATE_NO_PLATE):
            with patch("crawler.db.dart.set_dart_plate_state_pending", return_value=False):
                with pytest.raises(DartStateError):
                    add_dart_plate_if_doesnt_exist(mock_conn.cursor(), test_plate_barcode, test_labclass)

        # throws without state property
        with patch("crawler.db.dart.get_dart_plate_state", return_value=DART_STATE_NO_PROP):
            with pytest.raises(DartStateError):
                add_dart_plate_if_doesnt_exist(mock_conn.cursor(), test_plate_barcode, test_labclass)
