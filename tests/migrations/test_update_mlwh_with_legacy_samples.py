import sys
import traceback
from migrations.helpers.mlwh_samples_update_helper import (
    valid_datetime_string,
    update_mlwh_with_legacy_samples,
)
from crawler.constants import (
    FIELD_ROOT_SAMPLE_ID,
    FIELD_RNA_ID,
    FIELD_RESULT,
    FIELD_DATE_TESTED,
    FIELD_LAB_ID,
    FIELD_SOURCE,
    FIELD_PLATE_BARCODE,
    FIELD_CREATED_AT,
    FIELD_UPDATED_AT,
    MLWH_TABLE_NAME,
    MONGO_DATETIME_FORMAT,
)
import pytest
from datetime import (datetime, timedelta)

def print_exception(e: Exception) -> None:
    print(f'An exception occurred, at {datetime.now()}')
    e = sys.exc_info()
    print(e[0]) # exception type
    print(e[1]) # exception message
    if e[2]: # traceback
      traceback.print_tb(e[2], limit=10)

def generate_example_samples(range, start_datetime):
    samples = []
    for n in range:
        samples.append({
            FIELD_ROOT_SAMPLE_ID: "TLS0000000" + str(n),
            FIELD_RNA_ID: "TL-rna-00000001_A01",
            FIELD_RESULT: "Positive",
            FIELD_PLATE_BARCODE: "DN1000000" + str(n),
            FIELD_DATE_TESTED: "2020-05-10 14:01:00 UTC",
            FIELD_LAB_ID: "TLS",
            FIELD_SOURCE: "Test Lab Somewhere",
            FIELD_CREATED_AT: start_datetime + timedelta(days=n),
            FIELD_UPDATED_AT: start_datetime + timedelta(days=n),
        })

    print(samples)
    return samples

def clear_mlwh_table(mysql_conn):
    print("Clearing MLWH database table")

    cursor = mysql_conn.cursor()
    try:
        cursor.execute(f"TRUNCATE TABLE unified_warehouse_test.{MLWH_TABLE_NAME}")
        mysql_conn.commit()
        print('Table cleared')
    except Exception as e:
        print("An exception occurred clearing the table")
        print_exception(e)
    finally:
        print('Closing the cursor.')
        cursor.close()

def test_valid_datetime_string_invalid(config):
    assert(valid_datetime_string("") == False)
    assert(valid_datetime_string(None) == False)
    assert(valid_datetime_string("rubbish") == False)

def test_valid_datetime_string_valid(config):
    valid_dt = datetime.strftime(datetime.now(), MONGO_DATETIME_FORMAT)
    assert(valid_datetime_string(valid_dt) == True)

def test_basic_usage(mongo_database, mlwh_connection):
    config, mongo_db = mongo_database

    start_datetime = datetime(2020, 5, 10, 15, 10)

    # generate and insert sample rows into the mongo database
    test_samples = generate_example_samples(range(0, 6), start_datetime)
    mongo_db.samples.insert_many(test_samples)

    total_samples = mongo_db.samples.count_documents({})
    assert total_samples == 6

    # connect to the MySQL MLWH database
    _, mysql_conn = mlwh_connection

    # clear any existing rows in the lighthouse sample table
    clear_mlwh_table(mysql_conn)

    s_start_datetime = datetime.strftime(start_datetime, MONGO_DATETIME_FORMAT)
    s_end_datetime = datetime.strftime(start_datetime + timedelta(days=3), MONGO_DATETIME_FORMAT)

    # run the method to update the MLWH from the mongo database
    update_mlwh_with_legacy_samples(config, s_start_datetime, s_end_datetime)

    # query for selecting rows from MLWH (it was emptied before so select * is fine for this)
    sql_query = f"SELECT * FROM unified_warehouse_test.{MLWH_TABLE_NAME} ORDER BY {FIELD_CREATED_AT} ASC"

    try:
        # run the query and fetch the results
        cursor = mysql_conn.cursor()
        cursor.execute(sql_query)
        records = cursor.fetchall()

        # check there are the expected number of rows in the MLWH (4?)
        assert cursor.rowcount == 4

        # check the plate barcodes are as expected
        assert records[0][5] == "DN10000000"
        assert records[3][5] == "DN10000003"

    except Exception as e:
        print("An exception occurred checking the mlwh table for rows inserted")
        print_exception(e)
    finally:
        cursor.close()
        mysql_conn.close()

def test_when_no_rows_match_timestamp_range(mongo_database, mlwh_connection):
    config, mongo_db = mongo_database

    start_datetime = datetime(2020, 5, 10, 15, 10)

    # generate and insert sample rows into the mobgo database
    test_samples = generate_example_samples(range(0, 6), start_datetime)
    mongo_db.samples.insert_many(test_samples)

    total_samples = mongo_db.samples.count_documents({})
    assert total_samples == 6

    # connect to the MySQL MLWH database
    _, mysql_conn = mlwh_connection

    # clear any existing rows in the lighthouse sample table
    clear_mlwh_table(mysql_conn)

    # use timestamps such that no rows qualify
    s_start_datetime = datetime.strftime(start_datetime + timedelta(days=6), MONGO_DATETIME_FORMAT)
    s_end_datetime = datetime.strftime(start_datetime + timedelta(days=8), MONGO_DATETIME_FORMAT)

    # run the method to update the MLWH from the mongo database
    update_mlwh_with_legacy_samples(config, s_start_datetime, s_end_datetime)

    # query for selecting rows from MLWH (it was emptied before so select * is fine for this)
    sql_query = f"SELECT * FROM unified_warehouse_test.{MLWH_TABLE_NAME} ORDER BY {FIELD_CREATED_AT} ASC"

    try:
        # run the query and fetch the results
        cursor = mysql_conn.cursor()
        cursor.execute(sql_query)
        records = cursor.fetchall()

        # check there are the expected number of rows in the MLWH (0 fell within date range)
        assert cursor.rowcount == 0

    except Exception as e:
        print("An exception occurred checking the mlwh table for rows inserted")
        print_exception(e)
    finally:
        cursor.close()
        mysql_conn.close()
