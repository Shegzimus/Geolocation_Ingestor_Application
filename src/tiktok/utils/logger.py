
import json
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler


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
log_filename = f"airflow/logs/tiktok_{ts}.log"

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