"""
Structured logging configuration.

Provides consistent logging across all modules with structured output
suitable for both development and production environments.
"""

import logging
import sys
from datetime import datetime, timezone
from typing import Optional


# Default log format
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class UTCFormatter(logging.Formatter):
    """Formatter that uses UTC timestamps."""

    converter = lambda *args: datetime.now(timezone.utc).timetuple()

    def formatTime(self, record, datefmt=None):
        ct = datetime.fromtimestamp(record.created, tz=timezone.utc)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            s = ct.strftime(DATE_FORMAT)
        return s


def setup_logging(
    level: str = "INFO",
    log_format: Optional[str] = None,
    date_format: Optional[str] = None,
) -> None:
    """
    Configure logging for the application.
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Custom log format string
        date_format: Custom date format string
    """
    log_format = log_format or LOG_FORMAT
    date_format = date_format or DATE_FORMAT

    # Create formatter
    formatter = UTCFormatter(fmt=log_format, datefmt=date_format)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))

    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Add stdout handler
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    root_logger.addHandler(stdout_handler)

    # Reduce noise from httpx
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)

