"""
state_manager.py
----------------
Tracks the "last successful run" timestamp for incremental loading.

The pipeline reads this timestamp to know which BigQuery rows are NEW
(created after the last run). After a successful run, it saves the
new timestamp.

The state is stored in a simple text file (path from config.yaml).
"""

import os
import logging
from datetime import datetime, timezone

logger = logging.getLogger("doctor_management")

# A very old date - used on the very first run when no state exists yet.
# This makes the first run pick up ALL existing rows.
DEFAULT_START = "2000-01-01 00:00:00"


def read_last_run(state_file: str) -> str:
    """
    Read the last run timestamp from the state file.

    Args:
        state_file: Path to the state file.

    Returns:
        Timestamp string in format 'YYYY-MM-DD HH:MM:SS' (UTC).
        Returns a default old date if the file does not exist.
    """
    if not os.path.exists(state_file):
        logger.info(
            f"No state file found at {state_file}. "
            f"First run - will process all rows since {DEFAULT_START}"
        )
        return DEFAULT_START

    with open(state_file, "r") as f:
        timestamp = f.read().strip()

    if not timestamp:
        logger.warning("State file is empty. Using default start date.")
        return DEFAULT_START

    logger.info(f"Last run timestamp: {timestamp}")
    return timestamp


def write_last_run(state_file: str, timestamp: str = None) -> None:
    """
    Save the last run timestamp to the state file.

    Args:
        state_file: Path to the state file.
        timestamp: Timestamp string to save. If None, uses current UTC time.
    """
    # Make sure the folder exists
    state_dir = os.path.dirname(state_file)
    if state_dir and not os.path.exists(state_dir):
        os.makedirs(state_dir)

    if timestamp is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    with open(state_file, "w") as f:
        f.write(timestamp)

    logger.info(f"Saved last run timestamp: {timestamp}")


def get_current_utc() -> str:
    """Return current UTC time as a string 'YYYY-MM-DD HH:MM:SS'."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
