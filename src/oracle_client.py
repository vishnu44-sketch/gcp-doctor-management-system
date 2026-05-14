"""
oracle_client.py
----------------
Handles the connection to the Oracle database and writing data into it.

Uses the 'oracledb' library in thin mode (no Oracle Client install needed).
"""

import logging
import oracledb
import pandas as pd

logger = logging.getLogger("doctor_management")


def get_oracle_connection(config: dict):
    """
    Create and return an Oracle database connection.

    Args:
        config: Configuration dictionary (must contain 'oracle' section).

    Returns:
        An open Oracle connection object.
    """
    oracle_cfg = config["oracle"]

    host = oracle_cfg["host"]
    port = oracle_cfg["port"]
    service_name = oracle_cfg["service_name"]
    username = oracle_cfg["username"]
    password = oracle_cfg["password"]

    # Build the DSN (Data Source Name)
    dsn = oracledb.makedsn(host, port, service_name=service_name)

    logger.info(f"Connecting to Oracle at {host}:{port}/{service_name}")
    connection = oracledb.connect(user=username, password=password, dsn=dsn)
    logger.info("Connected to Oracle successfully")

    return connection


def table_exists(connection, table_name: str) -> bool:
    """
    Check if a table exists in Oracle.

    Args:
        connection: Open Oracle connection.
        table_name: Name of the table to check.

    Returns:
        True if the table exists, False otherwise.
    """
    cursor = connection.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM user_tables WHERE table_name = :tname",
        tname=table_name.upper()
    )
    count = cursor.fetchone()[0]
    cursor.close()
    return count > 0


def create_doctor_table(connection, table_name: str) -> None:
    """
    Create the doctor table in Oracle if it does not exist.

    The schema matches the BigQuery doctor table.

    Args:
        connection: Open Oracle connection.
        table_name: Name of the table to create.
    """
    cursor = connection.cursor()

    create_sql = f"""
        CREATE TABLE {table_name} (
            doctor_id         VARCHAR2(20),
            name              VARCHAR2(200),
            specialization    VARCHAR2(100),
            qualification     VARCHAR2(100),
            hospital          VARCHAR2(200),
            city              VARCHAR2(100),
            experience_years  NUMBER(3),
            email             VARCHAR2(200),
            phone             NUMBER(15),
            created_at        TIMESTAMP
        )
    """

    cursor.execute(create_sql)
    connection.commit()
    cursor.close()
    logger.info(f"Created Oracle table: {table_name}")


def ensure_table_exists(connection, table_name: str) -> None:
    """
    Make sure the target table exists. Create it if missing.

    Args:
        connection: Open Oracle connection.
        table_name: Name of the table.
    """
    if table_exists(connection, table_name):
        logger.info(f"Oracle table '{table_name}' already exists")
    else:
        logger.info(f"Oracle table '{table_name}' not found - creating it")
        create_doctor_table(connection, table_name)


def insert_dataframe(connection, table_name: str, df: pd.DataFrame) -> int:
    """
    Insert all rows from a DataFrame into the Oracle table.

    Uses executemany for efficient batch insert.
    Wrapped in a transaction - if any row fails, nothing is committed.

    Args:
        connection: Open Oracle connection.
        table_name: Target table name.
        df: DataFrame with doctor records to insert.

    Returns:
        Number of rows inserted.
    """
    if df.empty:
        logger.info("No rows to insert into Oracle")
        return 0

    cursor = connection.cursor()

    # Build the INSERT statement with named bind variables
    insert_sql = f"""
        INSERT INTO {table_name}
        (doctor_id, name, specialization, qualification, hospital,
         city, experience_years, email, phone, created_at)
        VALUES
        (:doctor_id, :name, :specialization, :qualification, :hospital,
         :city, :experience_years, :email, :phone, :created_at)
    """

    # Convert DataFrame rows into a list of dictionaries
    rows = df.to_dict(orient="records")

    try:
        cursor.executemany(insert_sql, rows)
        connection.commit()
        inserted = cursor.rowcount
        logger.info(f"Inserted {inserted} row(s) into Oracle table '{table_name}'")
        return inserted
    except Exception as e:
        connection.rollback()
        logger.error(f"Oracle insert failed - rolled back. Error: {e}")
        raise
    finally:
        cursor.close()


def close_connection(connection) -> None:
    """Close the Oracle connection cleanly."""
    if connection:
        connection.close()
        logger.info("Oracle connection closed")
