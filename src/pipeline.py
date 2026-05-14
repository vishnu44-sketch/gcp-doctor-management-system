"""
pipeline.py
-----------
The AUTOMATED pipeline script. This is what cron runs every 5 minutes.

It does NOT have a menu and does NOT ask for input - it runs start
to finish on its own.

Pipeline steps:
  1. Insert one random doctor into BigQuery (with created_at = now)
  2. Read BigQuery rows created since the last run (incremental)
  3. Insert those new rows into Oracle
  4. Save the current time as the new "last run" timestamp

If any step fails, the error is logged and the state file is NOT
updated - so the next run will retry the same rows.

Usage:
    python pipeline.py
"""

import sys

from src.logger import setup_logger
from src.bigquery_client import load_config, get_bigquery_client, get_table_id
from src.doctor_service import (
    generate_random_doctor,
    insert_doctor_with_timestamp,
    get_doctors_since,
)
from src.oracle_client import (
    get_oracle_connection,
    ensure_table_exists,
    insert_dataframe,
    close_connection,
)
from src.state_manager import read_last_run, write_last_run, get_current_utc


def run_pipeline():
    """Execute one full pipeline run."""

    # -------------------------------------------------------
    # Setup: config + logger
    # -------------------------------------------------------
    config = load_config("config/config.yaml")

    logger = setup_logger(
        log_file=config["logging"]["log_file"],
        log_level=config["logging"]["log_level"],
    )

    logger.info("=" * 50)
    logger.info("PIPELINE RUN STARTED")

    # Capture the run start time BEFORE doing anything.
    # We only save this as the new "last run" if everything succeeds.
    run_start_time = get_current_utc()

    bq_client = None
    oracle_conn = None

    try:
        # ---------------------------------------------------
        # Connect to BigQuery
        # ---------------------------------------------------
        bq_client = get_bigquery_client(config)
        table_id = get_table_id(config)

        # ---------------------------------------------------
        # STEP 1: Insert one random doctor into BigQuery
        # ---------------------------------------------------
        logger.info("STEP 1: Generating and inserting a random doctor")
        new_doctor = generate_random_doctor(bq_client, table_id)
        insert_doctor_with_timestamp(bq_client, table_id, new_doctor)

        # ---------------------------------------------------
        # STEP 2: Read incremental rows from BigQuery
        # ---------------------------------------------------
        logger.info("STEP 2: Reading new rows since last run")
        last_run = read_last_run(config["pipeline"]["state_file"])
        new_rows = get_doctors_since(bq_client, table_id, last_run)

        if new_rows.empty:
            logger.info("No new rows to load into Oracle. Pipeline done.")
            logger.info("PIPELINE RUN COMPLETED (nothing to load)")
            return

        # The newest created_at among fetched rows becomes the new state.
        # Format it as a UTC string for the state file.
        # Convert to UTC explicitly, then format.
        # This ensures the saved timestamp exactly matches what
        # BigQuery has stored, avoiding re-fetching the same row.
        max_created = new_rows["created_at"].max()
        max_created_utc = max_created.tz_convert("UTC") if max_created.tzinfo else max_created
        new_state_time = max_created_utc.strftime("%Y-%m-%d %H:%M:%S.%f")

        # ---------------------------------------------------
        # STEP 3: Insert new rows into Oracle
        # ---------------------------------------------------
        logger.info(f"STEP 3: Loading {len(new_rows)} row(s) into Oracle")
        oracle_conn = get_oracle_connection(config)

        oracle_table = config["oracle"]["table"]
        ensure_table_exists(oracle_conn, oracle_table)

        inserted = insert_dataframe(oracle_conn, oracle_table, new_rows)

        # ---------------------------------------------------
        # STEP 4: Save the new "last run" timestamp
        # Only happens if all steps above succeeded.
        # ---------------------------------------------------
        write_last_run(config["pipeline"]["state_file"], new_state_time)

        logger.info(
            f"PIPELINE RUN COMPLETED. "
            f"Inserted 1 row to BigQuery, {inserted} row(s) to Oracle."
        )

    except Exception as e:
        # Any failure: log it, do NOT update the state file.
        # Next run will retry the same window.
        logger.error(f"PIPELINE RUN FAILED: {e}")
        logger.error("State file NOT updated - next run will retry.")
        sys.exit(1)

    finally:
        # Always close connections, even if something failed
        if oracle_conn:
            close_connection(oracle_conn)
        logger.info("=" * 50)


if __name__ == "__main__":
    run_pipeline()
