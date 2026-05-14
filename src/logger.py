"""
logger.py
---------
Sets up logging for the entire application.
Logs are saved to a file AND printed to console with timestamps.
"""

import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logger(log_file: str = "logs/app.log", log_level: str = "INFO") -> logging.Logger:
    """
    Configure and return a logger that writes to both file and console.

    Args:
        log_file: Path where log messages will be saved.
        log_level: Minimum severity to log (DEBUG, INFO, WARNING, ERROR).

    Returns:
        A configured logger instance.
    """
    # Ensure the logs folder exists
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Create or get the logger
    logger = logging.getLogger("doctor_management")
    logger.setLevel(getattr(logging, log_level.upper()))

    # Prevent duplicate handlers if function called multiple times
    if logger.handlers:
        return logger

    # Format: "2026-05-13 10:30:45 | INFO | message"
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # File handler - writes logs to file (rotates when file gets too big)
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=1_000_000,   # 1 MB per file
        backupCount=3         # Keep last 3 files
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler - prints logs to terminal
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger
