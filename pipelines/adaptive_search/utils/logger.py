"""
Depth First Restaurant Search Logger Module
-------------------------------------------

This module configures a rotating, JSON-formatted logger for the
"depth_first_restaurant_search" application. Each run produces its own
timestamped log file, and log records are written as one-line JSON entries
with the following core fields:

  - timestamp: ISO‐formatted datetime string when the event occurred
  - level:     logging level name (DEBUG, INFO, WARNING, ERROR, CRITICAL)
  - logger:    the name of the logger that emitted the record
  - message:   the formatted log message

Any extra attributes you attach to log calls (via the `extra=` argument)
are automatically included in the JSON payload under their own keys.

Classes:
    JsonFormatter: Custom formatter that introspects a LogRecord and serializes
                   its data to JSON, omitting the standard logging attributes
                   listed in its `builtins` ignore set.

Globals:
    logger (logging.Logger): Module‐level logger set to DEBUG and using a
                             RotatingFileHandler.
    handler (RotatingFileHandler): Handler that writes up to 10 MB per file,
                                   keeps 5 backups, and rotates old logs.
    log_filename (str): Timestamped filename for the current run's log file.

Usage:
    Simply import this module; the logger will be configured on import.
    Then use:

        from depth_first_restaurant_search import logger

    and call:

        logger.debug("Starting search", extra={"search_id": sid})
        logger.info("Found %d restaurants", count, extra={"operation": "nearby_search"})
        logger.error("API request failed", exc_info=True)

    to produce clean, structured JSON logs that are easy to ingest into
    log analysis systems.
"""



import json
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler
import sys
from pathlib import Path
from typing import Optional

from ..config import LOGS_DIR

class JsonFormatter(logging.Formatter):
    def format(self, record):
        builtins = {
            "name", "msg", "args", "levelname", "levelno",
            "pathname", "filename", "module", "exc_info",
            "exc_text", "stack_info", "lineno", "funcName",
            "created", "msecs", "relativeCreated", "thread",
            "threadName", "processName", "process"
        }
        payload = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level":     record.levelname,
            "logger":    record.name,
            "message":   record.getMessage(),
        }
        # Pick up any extra attributes
        for key, value in record.__dict__.items():
            if key not in builtins:
                payload[key] = value
        return json.dumps(payload)

# ----------------------------------------------------------------------------------------------------------

# Timestamped filename
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"airflow/logs/restaurants/depth_first_restaurant_search_{ts}.log"

# ----------------------------------------------------------------------------------------------------------

# Create module‐level logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

handler = RotatingFileHandler(
    filename=log_filename,
    maxBytes=10_000_000,
    backupCount=5
)
handler.setFormatter(JsonFormatter())
logger.addHandler(handler)

# ----------------------------------------------------------------------------------------------------------

def setup_logger(
    name: str,
    level: int = logging.INFO,
    log_file: Optional[Path] = None
) -> logging.Logger:
    """
    Set up a logger with consistent formatting and handlers.
    
    Args:
        name: Name of the logger
        level: Logging level (default: INFO)
        log_file: Optional path to log file (default: LOGS_DIR/name.log)
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Create formatters
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(pathname)s:%(lineno)d'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # File handler
    if log_file is None:
        log_file = LOGS_DIR / f"{name}.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    return logger

# ----------------------------------------------------------------------------------------------------------
