import logging
from itertools import zip_longest
from typing import Any, Dict, Iterable, List, cast

import mysql.connector as mysql
import sqlalchemy
from mysql.connector.connection_cext import CMySQLConnection
from mysql.connector.cursor_cext import CMySQLCursor
from sqlalchemy.engine.base import Engine

from crawler.constants import MLWH_RNA_ID
from crawler.helpers.general_helpers import map_mongo_sample_to_mysql
from crawler.helpers.logging_helpers import LoggingCollection
from crawler.sql_queries import SQL_MLWH_MULTIPLE_INSERT, SQL_MLWH_UPDATE_MOST_RECENT_SAMPLE_COLUMNS
from crawler.types import Config, ModifiedRow

logger = logging.getLogger(__name__)


def create_mysql_connection(config: Config, readonly: bool = True) -> CMySQLConnection:
    """Create a CMySQLConnection with the given config parameters.

    Arguments:
        config (Config): application config specifying database details
        readonly (bool, optional): use the readonly credentials. Defaults to True.

    Returns:
        CMySQLConnection: a client used to interact with the database server
    """
    mlwh_db_host = config.MLWH_DB_HOST
    mlwh_db_port = config.MLWH_DB_PORT
    if readonly:
        mlwh_db_username = config.MLWH_DB_RO_USER
        mlwh_db_password = config.MLWH_DB_RO_PASSWORD
    else:
        mlwh_db_username = config.MLWH_DB_RW_USER
        mlwh_db_password = config.MLWH_DB_RW_PASSWORD
    mlwh_db_db = config.MLWH_DB_DBNAME

    logger.debug(f"Attempting to connect to {mlwh_db_host} on port {mlwh_db_port}")

    mysql_conn = None
    try:
        mysql_conn = mysql.connect(
            host=mlwh_db_host,
            port=mlwh_db_port,
            username=mlwh_db_username,
            password=mlwh_db_password,
            database=mlwh_db_db,
            # whether to use pure python or the C extension.
            # default is false, but specify it so more predictable
            use_pure=False,
        )
        if mysql_conn is not None:
            if mysql_conn.is_connected():
                logger.debug("MySQL Connection Successful")
            else:
                logger.error("MySQL Connection Failed")

    except mysql.Error as e:
        logger.error(f"Exception on connecting to MySQL database: {e}")

    return cast(CMySQLConnection, mysql_conn)


def run_mysql_executemany_query(mysql_conn: CMySQLConnection, sql_query: str, values: List[Dict[str, str]]) -> None:
    """Writes the sample testing information into the MLWH.

    Arguments:
        mysql_conn {CMySQLConnection} -- a client used to interact with the database server
        sql_query {str} -- the SQL query to run (see sql_queries.py)
        values {List[Dict[str, str]]} -- array of value hashes representing documents inserted into
        the Mongo DB
    """
    # fetch the cursor from the DB connection
    cursor: CMySQLCursor = mysql_conn.cursor()

    try:
        # executing the query with values
        num_values = len(values)

        # BN. If ROWS_PER_QUERY value is too high, you may get '2006 (HY000): MySQL server has
        # gone away' error indicating you've exceeded the max_allowed_packet size for MySQL
        ROWS_PER_QUERY = 15000
        values_index = 0
        total_rows_affected = 0
        logger.debug(
            f"Attempting to insert or update {num_values} rows in the MLWH database in batches of {ROWS_PER_QUERY}"
        )

        while values_index < num_values:
            logger.debug(f"Inserting records between {values_index} and {values_index + ROWS_PER_QUERY}")
            cursor.executemany(sql_query, values[values_index : (values_index + ROWS_PER_QUERY)])  # noqa: E203
            logger.debug(
                f"{cursor.rowcount} rows affected in MLWH. (Note: each updated row increases the "
                "count by 2, instead of 1)"
            )
            total_rows_affected += cursor.rowcount
            values_index += ROWS_PER_QUERY
            logger.debug("Committing changes to MLWH database.")
            mysql_conn.commit()

        # number of rows affected using cursor.rowcount - not easy to interpret:
        # reports 1 per inserted row,
        # 2 per updated existing row,
        # and 0 per unchanged existing row
        logger.info(
            f"A total of {total_rows_affected} rows were affected in MLWH. (Note: each updated row "
            "increases the count by 2, instead of 1)"
        )
        rna_ids = [sample[MLWH_RNA_ID] for sample in values if MLWH_RNA_ID in sample]
        update_most_recent_rna_ids(cursor, rna_ids)
        mysql_conn.commit()

    except Exception:
        logger.error("MLWH database executemany transaction failed")
        raise
    finally:
        # close the cursor
        logger.debug("Closing the cursor.")
        cursor.close()

        # close the connection
        logger.debug("Closing the MLWH database connection.")
        mysql_conn.close()


def run_mysql_execute_formatted_query(
    mysql_conn: CMySQLConnection, formatted_sql_query: str, formatting_args: List[str], query_args: List[Any]
) -> None:
    """Executes formatted sql query, unwrapping and batching based on number of input arguments

    Arguments:
        mysql_conn {CMySQLConnection} -- a client used to interact with the database server
        formatted_sql_query {str} -- the formatted SQL query to run (unwrapped using % workflow)
        formatting_args {List[str]} -- arguments to batch and unwrap the formatted sql query
        query_args {List[Any]} -- additional sql query arguments
    """
    # fetch the cursor from the DB connection
    cursor = mysql_conn.cursor()

    try:
        # executing the query with values
        num_formatting_args = len(formatting_args)

        # BN. If FORMATTING_ARGS_PER_QUERY value is too high, you may get '2006 (HY000): MySQL server has
        # gone away' error indicating you've exceeded the max_allowed_packet size for MySQL
        FORMATTING_ARGS_PER_QUERY = 15000
        formatting_args_index = 0
        total_rows_affected = 0
        logger.debug(
            f"Attempting to execute formatted sql on the MLWH database in batches of {FORMATTING_ARGS_PER_QUERY}"
        )

        while formatting_args_index < num_formatting_args:
            logger.debug(
                f"Executing sql for formatting args between {formatting_args_index} and \
{formatting_args_index + FORMATTING_ARGS_PER_QUERY}"
            )

            formatting_args_batch = formatting_args[
                formatting_args_index : (formatting_args_index + FORMATTING_ARGS_PER_QUERY)  # noqa: E203
            ]

            sql_unwrap_formatted_args = ", ".join(
                list(map(lambda x: "%s", formatting_args_batch))
            )  # e.g. for 3 ids, this would look like "%s,%s,%s"

            if len(formatting_args_batch) > 0:
                sql_query = (
                    formatted_sql_query % sql_unwrap_formatted_args
                )  # formats the query to have the correct number of formatting arguments for the ids
                string_args = (
                    query_args + formatting_args_batch
                )  # adds the filtered positive arguments to the id arguments
                cursor.execute(sql_query, tuple(string_args))

            total_rows_affected += cursor.rowcount
            logger.debug(f"{cursor.rowcount} rows affected in MLWH.")

            formatting_args_index += FORMATTING_ARGS_PER_QUERY
            logger.debug("Committing changes to MLWH database.")
            mysql_conn.commit()

        logger.debug(f"Successfully affected a total of {total_rows_affected} rows in MLWH.")
    except Exception:
        logger.error("MLWH database execute transaction failed")
        raise
    finally:
        # close the cursor
        logger.debug("Closing the cursor.")
        cursor.close()


def create_mysql_connection_engine(connection_string: str, database: str = "") -> Engine:
    """Creates a SQLAlchemy engine from the connection string and optional database.

    Arguments:
        connection_string (str): connection string containing host, port, username and password.
        database (str, optional): name of the database to connect to. Defaults to "".

    Returns:
        Engine: SQLAlchemy engine to use for querying the MySQL database.
    """
    create_engine_string = f"mysql+pymysql://{connection_string}"

    if database:
        create_engine_string += f"/{database}"

    return sqlalchemy.create_engine(create_engine_string, pool_recycle=3600)


def mygrouper(size_group: int, iterable: Iterable) -> List[str]:
    """Creates group of size_group size from the list defined by the iterable.

    Arguments:
        size_group (int): maximum number of elements for each group (the last group could have
        less elements to fit)
        iterable (Iterable): iterator on the list we want to split in groups
    """
    args = [iter(iterable)] * size_group
    return ([e for e in t if e is not None] for t in zip_longest(*args))


def format_sql_list_str(mylist: List[str]) -> str:
    """Writes the provided list as a SQL list of strings.

    Arguments:
        mylist (List<str>): list of strings that we want to format in SQL
    """
    if len(mylist) == 0:
        return "()"
    if len(mylist) == 1:
        return f"('{ mylist[0]}')"
    else:
        return str(tuple(mylist))


def update_most_recent_rna_ids(cursor: CMySQLCursor, rna_ids: List[str], chunk_size: int = 1000) -> None:
    """Receives a cursor with an active connection and a list of rna_ids and
    run an update on the list of rna ids in groups of chunk_size

    Arguments:
        cursor: database cursor with an active connection
        rna_ids: List of strings with the rna ids where we want to update the most recent columns
        chunk_size: Size of the groups in which we will process this update.
    """
    rna_ids_groups = mygrouper(chunk_size, rna_ids)

    total_rows_affected = 0
    for rna_ids_group in rna_ids_groups:
        cursor.execute(SQL_MLWH_UPDATE_MOST_RECENT_SAMPLE_COLUMNS % format_sql_list_str(rna_ids_group))
        total_rows_affected += cursor.rowcount

    logger.info(f"Updated { total_rows_affected } rows for most_recent_rna_ids")


def insert_or_update_samples_in_mlwh(
    samples: List[ModifiedRow],
    config: Config,
    logging_collection: LoggingCollection,
    logging_messages: Dict[str, Dict[str, Any]],
) -> bool:
    """Insert or update sample records into the MLWH database from the given samples, including the corresponding
    mongodb `_id`, `must_sequence` and `preferentially_sequence` for priority samples.

    Arguments:
        samples {List[ModifiedRow]} -- List of sample information
        config {Config} -- Config object
        logging_collection {LoggingCollection} -- the logging collection to use for logging
        logging_messages {Dict} -- a dictionary containing the logging messages to use for logging

    Returns:
        {bool} -- True if the insert was successful; otherwise False
    """
    values = list(map(map_mongo_sample_to_mysql, samples))
    mysql_conn = create_mysql_connection(config=config, readonly=False)

    if mysql_conn is not None and mysql_conn.is_connected():
        try:
            run_mysql_executemany_query(mysql_conn=mysql_conn, sql_query=SQL_MLWH_MULTIPLE_INSERT, values=values)

            logger.debug(logging_messages["success"]["msg"])
            return True
        except Exception as e:
            logging_collection.add_error(
                logging_messages["insert_failure"]["error_type"], logging_messages["insert_failure"]["msg"]
            )
            logger.critical(f"{logging_messages['insert_failure']['critical_msg']}: {e}")
            logger.exception(e)
    else:
        logging_collection.add_error(
            logging_messages["connection_failure"]["error_type"],
            logging_messages["connection_failure"]["msg"],
        )
        logger.critical(logging_messages["connection_failure"]["critical_msg"])

    return False
