import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict

from core.log_redaction import install_log_redaction, redact_exception_text, redact_value

# Standard Python Log Levels
# CRITICAL = 50
# ERROR = 40
# WARNING = 30
# INFO = 20
# DEBUG = 10
# NOTSET = 0

class JsonFormatter(logging.Formatter):
    """
    Formatter that outputs JSON strings for structured logging.
    """
    def format(self, record: logging.LogRecord) -> str:
        log_record: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "message": redact_value(record.getMessage()),
            "module": record.module,
            "funcName": record.funcName,
            "line": record.lineno,
            "logger": record.name
        }
        
        # Merge extra field if available
        if hasattr(record, 'context') and isinstance(record.context, dict): # type: ignore
            log_record.update(redact_value(record.context)) # type: ignore
            
        # Exception handling
        if record.exc_info:
            exception_text = record.exc_text or self.formatException(record.exc_info)
            log_record["exception"] = redact_exception_text(exception_text)
            
        return json.dumps(redact_value(log_record))


class RedactingFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return redact_sensitive_text(super().format(record))

def configure_logging() -> logging.Logger:
    """
    Configures the root logger based on environment variables.
    ENV: LOG_FORMAT (JSON | TEXT) - Required
    ENV: LOG_LEVEL (DEBUG | INFO | WARNING | ERROR) - Required
    """
    install_log_redaction()

    logger = logging.getLogger()
    
    # idempotent configuration
    if logger.handlers:
        return logger
        
    log_format_raw = os.environ.get("LOG_FORMAT")
    if log_format_raw is None or not log_format_raw.strip():
        raise ValueError("LOG_FORMAT is required (set to JSON or TEXT).")
    log_format = log_format_raw.strip().upper()

    log_level_raw = os.environ.get("LOG_LEVEL")
    if log_level_raw is None or not log_level_raw.strip():
        raise ValueError("LOG_LEVEL is required (DEBUG|INFO|WARNING|ERROR).")
    log_level_str = log_level_raw.strip().upper()

    if log_format not in {"JSON", "TEXT"}:
        raise ValueError(f"Invalid LOG_FORMAT={log_format_raw!r} (expected JSON or TEXT).")

    try:
        level = getattr(logging, log_level_str)
    except AttributeError as exc:
        raise ValueError(
            f"Invalid LOG_LEVEL={log_level_raw!r} (expected DEBUG|INFO|WARNING|ERROR|CRITICAL)."
        ) from exc
    if not isinstance(level, int):
        raise ValueError(
            f"Invalid LOG_LEVEL={log_level_raw!r} (expected DEBUG|INFO|WARNING|ERROR|CRITICAL)."
        )
    logger.setLevel(level)
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    
    if log_format == "JSON":
        handler.setFormatter(JsonFormatter())
    else:
        # Standard readable format
        formatter = RedactingFormatter(
            '%(asctime)s [%(levelname)s] %(module)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        
    logger.addHandler(handler)
    
    # Silence Azure SDK spam
    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    # Silence http client request logs (can include query-string secrets like apikey=...).
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    
    return logger
