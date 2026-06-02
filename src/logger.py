"""Structured JSON logging for pipeline."""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional


def convert_to_json_serializable(obj: Any) -> Any:
    """Convert non-JSON-serializable types to native Python types."""
    import numpy as np
    import pandas as pd
    
    if isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, pd.Series):
        return obj.tolist()
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, datetime):
        return obj.isoformat()
    return obj


class StructuredFormatter(logging.Formatter):
    """JSON structured log formatter."""
    
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "module": record.module,
            "message": record.getMessage(),
        }
        
        # Add extra fields if present
        if hasattr(record, "extra"):
            # Convert all values to JSON-serializable types
            converted_extra = {}
            for key, value in record.extra.items():
                converted_extra[key] = convert_to_json_serializable(value)
            log_entry.update(converted_extra)
        
        return json.dumps(log_entry)


def setup_logger(
    name: str = "pipeline",
    log_file: Optional[str] = "logs/pipeline_phase0_phase1.log",
    level: int = logging.INFO,
) -> logging.Logger:
    """Set up structured JSON logger."""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # File handler (JSON)
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(StructuredFormatter())
        logger.addHandler(file_handler)
    
    # Console handler (human-readable)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(module)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    logger.addHandler(console_handler)
    
    return logger


def log_progress(
    logger: logging.Logger,
    message: str,
    processed: int,
    total: int,
    **kwargs: Any,
) -> None:
    """Log progress with percentage."""
    pct = (processed / total * 100) if total > 0 else 0
    logger.info(
        f"{message} ({processed:,}/{total:,} - {pct:.1f}%)",
        extra={"extra": {"processed": processed, "total": total, "pct": pct, **kwargs}}
    )


def log_metric(
    logger: logging.Logger,
    metric_name: str,
    value: Any,
    **kwargs: Any,
) -> None:
    """Log a metric value."""
    # Convert value to JSON-serializable
    value = convert_to_json_serializable(value)
    
    logger.info(
        f"METRIC {metric_name}={value}",
        extra={"extra": {"metric": metric_name, "value": value, **kwargs}}
    )
