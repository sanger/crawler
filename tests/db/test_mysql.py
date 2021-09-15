from datetime import datetime
from unittest.mock import MagicMock, patch

import mysql.connector as mysql
import pytest
from mysql.connector.connection_cext import CMySQLConnection
from sqlalchemy.engine.base import Engine

from crawler.db.mysql import (
    create_mysql_connection,
    create_mysql_connection_engine,
    partition,
    reset_is_current_flags,
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


def test_partition():
    assert list(partition([1, 2, 3, 4, 5], 3)) == [[1, 2, 3], [4, 5]]
    assert list(partition([], 3)) == []
    assert list(partition([1], 3)) == [[1]]
    assert list(partition([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)) == [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    assert list(partition([1, 2, 3, 4, 5, 6, 7, 8, 9], 4)) == [[1, 2, 3, 4], [5, 6, 7, 8], [9]]


def test_reset_is_current_flags(config):
    sql_engine = create_mysql_connection_engine(config.WAREHOUSES_RW_CONN_STRING, config.ML_WH_DB)
    connection = sql_engine.raw_connection()

    cursor = connection.cursor()

    def insert_lighthouse_sample(cursor, root_sample_id, rna_id, result, is_current):
        cursor.execute(
            (
                f"INSERT INTO lighthouse_sample (root_sample_id, rna_id, result, updated_at, is_current)"
                f" VALUES ('{root_sample_id}', '{rna_id}', '{result}', '2020-01-02 03:04:05', {is_current});"
            )
        )

    try:
        cursor.execute("DELETE FROM lighthouse_sample;")

        insert_lighthouse_sample(cursor, "rna_1", "rna_A01", "negative", 0)
        insert_lighthouse_sample(cursor, "rna_2", "rna_A01", "positive", 1)
        insert_lighthouse_sample(cursor, "rna_3", "rna_A02", "positive", 0)
        insert_lighthouse_sample(cursor, "rna_4", "rna_A02", "negative", 1)
        insert_lighthouse_sample(cursor, "rna_5", "rna_A03", "positive", 1)

        connection.commit()

        cursor.execute("SELECT root_sample_id FROM lighthouse_sample WHERE is_current=0;")
        rows = [row[0] for row in cursor.fetchall()]
        assert rows == ["rna_1", "rna_3"]

        cursor.execute("SELECT root_sample_id FROM lighthouse_sample WHERE is_current=1;")
        rows = [row[0] for row in cursor.fetchall()]
        assert rows == ["rna_2", "rna_4", "rna_5"]

        cursor.execute("SELECT DISTINCT(updated_at) FROM lighthouse_sample;")
        assert len(cursor.fetchall()) == 1

        reset_is_current_flags(cursor, ["rna_A01", "rna_A03"])
        connection.commit()

        cursor.execute("SELECT root_sample_id FROM lighthouse_sample WHERE is_current=0;")
        rows = [row[0] for row in cursor.fetchall()]
        assert rows == ["rna_1", "rna_2", "rna_3", "rna_5"]

        cursor.execute("SELECT root_sample_id FROM lighthouse_sample WHERE is_current=1;")
        rows = [row[0] for row in cursor.fetchall()]
        assert rows == ["rna_4"]

        cursor.execute("SELECT DISTINCT(updated_at) FROM lighthouse_sample;")
        assert len(cursor.fetchall()) == 2

    finally:
        cursor.execute("DELETE FROM lighthouse_sample;")
        connection.close()
