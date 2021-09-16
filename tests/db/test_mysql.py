from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import mysql.connector as mysql
import pytest
from mysql.connector.connection_cext import CMySQLConnection
from sqlalchemy.engine.base import Engine

from crawler.constants import MLWH_IS_CURRENT
from crawler.db.mysql import (
    create_mysql_connection,
    create_mysql_connection_engine,
    insert_or_update_samples_in_mlwh,
    partition,
    reset_is_current_flags,
    run_mysql_execute_formatted_query,
    run_mysql_executemany_query,
)
from crawler.helpers.logging_helpers import LoggingCollection
from crawler.sql_queries import SQL_MLWH_MULTIPLE_FILTERED_POSITIVE_UPDATE_BATCH, SQL_MLWH_MULTIPLE_INSERT
from tests.testing_objects import MLWH_SAMPLE_COMPLETE


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


def test_reset_is_current_flags(mlwh_rw_db):
    connection, cursor = mlwh_rw_db

    def insert_lighthouse_sample(root_sample_id, rna_id, result, is_current):
        cursor.execute(
            (
                f"INSERT INTO lighthouse_sample (root_sample_id, rna_id, result, updated_at, is_current)"
                f" VALUES ('{root_sample_id}', '{rna_id}', '{result}', '2020-01-02 03:04:05', {is_current});"
            )
        )

    insert_lighthouse_sample("rna_1", "rna_A01", "negative", 0)
    insert_lighthouse_sample("rna_2", "rna_A01", "positive", 1)
    insert_lighthouse_sample("rna_3", "rna_A02", "positive", 0)
    insert_lighthouse_sample("rna_4", "rna_A02", "negative", 1)
    insert_lighthouse_sample("rna_5", "rna_A03", "positive", 1)

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


def test_insert_samples_in_mlwh_inserts_one_complete_sample_correctly(config, mlwh_rw_db, logging_messages):
    _, cursor = mlwh_rw_db

    with patch("crawler.db.mysql.map_mongo_sample_to_mysql"):
        with patch("crawler.db.mysql.set_is_current_on_mysql_samples") as make_mysql_samples:
            make_mysql_samples.return_value = [MLWH_SAMPLE_COMPLETE]
            insert_or_update_samples_in_mlwh([{"pseudo": "sample"}], config, LoggingCollection(), logging_messages)

    fields = [
        "ch1_cq",
        "ch1_result",
        "ch1_target",
        "ch2_cq",
        "ch2_result",
        "ch2_target",
        "ch3_cq",
        "ch3_result",
        "ch3_target",
        "ch4_cq",
        "ch4_result",
        "ch4_target",
        "coordinate",
        "date_tested",
        "filtered_positive",
        "filtered_positive_timestamp",
        "filtered_positive_version",
        "is_current",
        "lab_id",
        "lh_sample_uuid",
        "lh_source_plate_uuid",
        "mongodb_id",
        "must_sequence",
        "plate_barcode",
        "preferentially_sequence",
        "result",
        "rna_id",
        "root_sample_id",
        "source",
    ]
    cursor.execute(f"SELECT {','.join(fields)} FROM lighthouse_sample;")
    rows = [row for row in cursor.fetchall()]
    assert len(rows) == 1

    row = rows[0]
    assert row == (
        Decimal("24.67"),
        "Positive",
        "A gene",
        Decimal("23.92"),
        "Negative",
        "B gene",
        Decimal("25.12"),
        "Positive",
        "C gene",
        Decimal("22.86"),
        "Negative",
        "D gene",
        "C3",
        datetime(2021, 2, 3, 4, 5, 6),
        True,
        datetime(2021, 2, 3, 5, 6, 7),
        "v3",
        True,
        "BB",
        "233223d5-9015-4646-add0-f358ff2688c7",
        "c6410270-5cbf-4233-a8d1-b08445bbac5e",
        "6140f388800f8fe309689124",
        True,
        "95123456789012345",
        False,
        "Positive",
        "95123456789012345_C03",
        "BAA94123456",
        "Bob's Biotech",
    )


def test_update_samples_in_mlwh_sets_is_current_correctly(config, mlwh_rw_db, logging_messages):
    _, cursor = mlwh_rw_db

    # Run two insert_or_updates back to back for the same document
    # This may seem like a redundant test, but because the second call is an update rather than in insert
    # the way it is processed is different.  It was observed that samples being updated to be priority samples
    # were losing the flag for is_current.  This was set explicitly to False as part of the insert preparation and
    # then the update was not pushing the value back to True again.
    with patch("crawler.db.mysql.map_mongo_sample_to_mysql"):
        with patch("crawler.db.mysql.set_is_current_on_mysql_samples") as make_mysql_samples:
            make_mysql_samples.return_value = [MLWH_SAMPLE_COMPLETE]
            insert_or_update_samples_in_mlwh([{"pseudo": "sample"}], config, LoggingCollection(), logging_messages)
            insert_or_update_samples_in_mlwh([{"pseudo": "sample"}], config, LoggingCollection(), logging_messages)

    cursor.execute(f"SELECT {MLWH_IS_CURRENT} FROM lighthouse_sample;")
    rows = [row for row in cursor.fetchall()]
    assert len(rows) == 1
    assert rows[0][0] == 1
