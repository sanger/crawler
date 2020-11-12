from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from crawler.constants import (
    DART_GET_PLATE_PROPERTY_SQL,
    DART_SET_PLATE_PROPERTY_SQL,
    DART_STATE_PENDING,
    DART_STATE_PROPERTY_NAME,
)
from crawler.db import (
    create_dart_sql_server_conn,
    create_import_record,
    create_mongo_client,
    create_mysql_connection,
    get_dart_plate_state,
    get_mongo_collection,
    get_mongo_db,
    run_mysql_executemany_query,
    set_dart_plate_state_pending,
)
from crawler.helpers import LoggingCollection
from crawler.sql_queries import SQL_MLWH_MULTIPLE_INSERT
from mysql.connector.connection_cext import CMySQLConnection
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database


def test_create_mongo_client(config):
    assert type(create_mongo_client(config)) == MongoClient


def test_get_mongo_db(mongo_client):
    config, mongo_client = mongo_client
    assert type(get_mongo_db(config, mongo_client)) == Database


def test_get_mongo_collection(mongo_database):
    _, mongo_database = mongo_database
    collection_name = "test_collection"
    test_collection = get_mongo_collection(mongo_database, collection_name)
    assert type(test_collection) == Collection
    assert test_collection.name == collection_name


def test_create_import_record(freezer, mongo_database):
    config, mongo_database = mongo_database
    import_collection = mongo_database["imports"]

    docs = [{"x": 1}, {"y": 2}, {"z": 3}]
    error_collection = LoggingCollection()
    error_collection.add_error("TYPE 4", "error1")
    error_collection.add_error("TYPE 5", "error2")

    for centre in config.CENTRES:
        now = datetime.now().isoformat(timespec="seconds")
        result = create_import_record(
            import_collection, centre, len(docs), "test", error_collection.get_messages_for_import()
        )
        import_doc = import_collection.find_one({"_id": result.inserted_id})

        assert import_doc["date"] == now
        assert import_doc["centre_name"] == centre["name"]
        assert import_doc["csv_file_used"] == "test"
        assert import_doc["number_of_records"] == len(docs)
        assert import_doc["errors"] == error_collection.get_messages_for_import()


def test_create_mysql_connection_none(config):
    with patch("mysql.connector.connect", return_value=None):
        assert create_mysql_connection(config) is None


def test_create_mysql_connection_exception(config):
    # For example, if the credentials in the config are wrong
    with patch("mysql.connector.connect", side_effect=mysql.Error()):
        assert create_mysql_connection(config) == None


def test_run_mysql_executemany_query_success(config):
    conn = CMySQLConnection()

    conn.cursor = MagicMock()
    conn.commit = MagicMock()
    conn.rollback = MagicMock()
    conn.close = MagicMock()

    cursor = conn.cursor.return_value
    cursor.executemany = MagicMock()
    cursor.close = MagicMock()

    run_mysql_executemany_query(
        mysql_conn=conn, sql_query=SQL_MLWH_MULTIPLE_INSERT, values=["test"]
    )

    # check transaction is committed
    assert conn.commit.called is True

    # check connection is closed
    assert cursor.close.called is True
    assert conn.close.called is True


def test_run_mysql_executemany_query_execute_error(config):
    conn = CMySQLConnection()

    conn.cursor = MagicMock()
    conn.commit = MagicMock()
    conn.rollback = MagicMock()
    conn.close = MagicMock()

    cursor = conn.cursor.return_value
    cursor.executemany = MagicMock(side_effect=Exception("Boom!"))
    cursor.close = MagicMock()

    with pytest.raises(Exception):
        run_mysql_executemany_query(
            mysql_conn=conn, sql_query=SQL_MLWH_MULTIPLE_INSERT, values=["test"]
        )

        # check transaction is not committed
        assert conn.commit.called is False

        # check connection is closed
        assert cursor.close.called is True
        assert conn.close.called is True


def test_create_dart_sql_server_conn_readonly(config):
    with patch("pyodbc.connect") as mock_connect:
        conn_string = (
            f"DRIVER={config.DART_DB_DRIVER};SERVER={config.DART_DB_HOST};"
            f"PORT={config.DART_DB_PORT};DATABASE={config.DART_DB_DBNAME};"
            f"UID={config.DART_DB_RO_USER};PWD={config.DART_DB_RO_PASSWORD}"
        )
        create_dart_sql_server_conn(config)
        mock_connect.assert_called_with(conn_string)


def test_create_dart_sql_server_conn_readwrite(config):
    with patch("pyodbc.connect") as mock_connect:
        conn_string = (
            f"DRIVER={config.DART_DB_DRIVER};SERVER={config.DART_DB_HOST};"
            f"PORT={config.DART_DB_PORT};DATABASE={config.DART_DB_DBNAME};"
            f"UID={config.DART_DB_RW_USER};PWD={config.DART_DB_RW_PASSWORD}"
        )
        create_dart_sql_server_conn(config, readonly=False)
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
        assert (
            get_dart_plate_state(mock_conn.cursor(), test_plate_barcode)
            == mock_conn.cursor().fetchval()
        )
        mock_conn.cursor().execute.assert_called_with(
            DART_GET_PLATE_PROPERTY_SQL, (test_plate_barcode, DART_STATE_PROPERTY_NAME)
        )


def test_set_dart_plate_state_pending(config):
    with patch("pyodbc.connect") as mock_conn:

        test_plate_barcode = "AB123"
        set_dart_plate_state_pending(mock_conn.cursor(), test_plate_barcode)
        mock_conn.cursor().execute.assert_called_with(
            DART_SET_PLATE_PROPERTY_SQL,
            (test_plate_barcode, DART_STATE_PROPERTY_NAME, DART_STATE_PENDING),
        )
