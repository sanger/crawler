from unittest.mock import MagicMock, patch

import mysql.connector as mysql
import pytest
from mysql.connector.connection_cext import CMySQLConnection
from sqlalchemy.engine.base import Engine

from crawler.db.mysql import (
    create_mysql_connection,
    create_mysql_connection_engine,
    run_mysql_execute_formatted_query,
    run_mysql_executemany_query,
)
from crawler.sql_queries import SQL_MLWH_MULTIPLE_FILTERED_POSITIVE_UPDATE_BATCH, SQL_MLWH_MULTIPLE_INSERT


def test_create_mysql_connection_none(config):
    with patch("mysql.connector.connect", return_value=None):
        assert create_mysql_connection(config) is None


def test_create_mysql_connection_exception(config):
    # For example, if the credentials in the config are wrong
    with patch("mysql.connector.connect", side_effect=mysql.Error()):
        assert create_mysql_connection(config) is None


def test_run_mysql_executemany_query_success(config):
    conn = CMySQLConnection()

    conn.cursor = MagicMock()
    conn.commit = MagicMock()
    conn.rollback = MagicMock()
    conn.close = MagicMock()

    cursor = conn.cursor.return_value
    cursor.executemany = MagicMock()
    cursor.close = MagicMock()

    run_mysql_executemany_query(mysql_conn=conn, sql_query=SQL_MLWH_MULTIPLE_INSERT, values=[{}])

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
            mysql_conn=conn, sql_query=SQL_MLWH_MULTIPLE_INSERT, values=["test"]  # type: ignore
        )

        # check transaction is not committed
        assert conn.commit.called is False

        # check connection is closed
        assert cursor.close.called is True
        assert conn.close.called is True


def test_run_mysql_execute_formatted_query_success(config):
    conn = CMySQLConnection()

    conn.cursor = MagicMock()
    conn.commit = MagicMock()
    conn.rollback = MagicMock()
    conn.close = MagicMock()

    cursor = conn.cursor.return_value
    cursor.execute = MagicMock()
    cursor.close = MagicMock()

    run_mysql_execute_formatted_query(
        mysql_conn=conn,
        formatted_sql_query=SQL_MLWH_MULTIPLE_FILTERED_POSITIVE_UPDATE_BATCH,
        formatting_args=["1", "2"],
        query_args=[True, "v2", "2020-01-01", "2020-01-01"],
    )

    # check transaction is committed
    assert conn.commit.called is True

    # check connection is closed
    assert cursor.close.called is True


def test_run_mysql_execute_formatted_query_execute_error(config):
    conn = CMySQLConnection()

    conn.cursor = MagicMock()
    conn.commit = MagicMock()
    conn.rollback = MagicMock()
    conn.close = MagicMock()

    cursor = conn.cursor.return_value
    cursor.execute = MagicMock(side_effect=Exception("Boom!"))
    cursor.close = MagicMock()

    with pytest.raises(Exception):
        run_mysql_execute_formatted_query(
            mysql_conn=conn,
            formatted_sql_query=SQL_MLWH_MULTIPLE_FILTERED_POSITIVE_UPDATE_BATCH,
            formatting_args=["1", "2"],
            query_args=[True, "v2", "2020-01-01", "2020-01-01"],
        )

        # check transaction is not committed
        assert conn.commit.called is False

        # check connection is closed
        assert cursor.close.called is True


def test_create_mysql_connection_engine_returns_expected(config):
    sql_engine = create_mysql_connection_engine(config.WAREHOUSES_RW_CONN_STRING, config.ML_WH_DB)
    assert isinstance(sql_engine, Engine)


def test_create_mysql_connection_engine_result_can_initiate_connection(config):
    sql_engine = create_mysql_connection_engine(config.WAREHOUSES_RW_CONN_STRING, config.ML_WH_DB)
    connection = sql_engine.connect()

    assert connection.closed is False
