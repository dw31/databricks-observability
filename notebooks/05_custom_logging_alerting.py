# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 5: Custom Application Logging & Alerting Framework
# MAGIC
# MAGIC Standardize Python-based error handling and log aggregation for tasks outside of
# MAGIC DLT or system tables. Implements structured JSON logging with centralized storage.
# MAGIC
# MAGIC **Implementation:**
# MAGIC - Python `logging` module with DEBUG through CRITICAL levels
# MAGIC - Structured JSON logging with ISO-8601 timestamps
# MAGIC - Centralized log storage via DBFS FileHandler
# MAGIC - Exception handling with `try-except` blocks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("log_path", "/dbfs/FileStore/logs/observability", "DBFS Log Path")
dbutils.widgets.dropdown("log_level", "INFO", ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], "Log Level")
dbutils.widgets.text("app_name", "databricks-observability", "Application Name")

LOG_PATH = dbutils.widgets.get("log_path")
LOG_LEVEL = dbutils.widgets.get("log_level")
APP_NAME = dbutils.widgets.get("app_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Structured JSON Logger Setup

# COMMAND ----------

import logging
import json
import os
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler


class StructuredJsonFormatter(logging.Formatter):
    """Formats log records as structured JSON with ISO-8601 timestamps."""

    def __init__(self, app_name: str = "databricks-observability"):
        super().__init__()
        self.app_name = app_name

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S"
            ),
            "level": record.levelname,
            "logger": record.name,
            "app_name": self.app_name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add exception info if present
        if record.exc_info and record.exc_info[0] is not None:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": self.formatException(record.exc_info),
            }

        # Add extra fields (excluding standard LogRecord attributes)
        standard_attrs = {
            "name", "msg", "args", "created", "relativeCreated", "exc_info",
            "exc_text", "stack_info", "lineno", "funcName", "pathname",
            "filename", "module", "thread", "threadName", "process",
            "processName", "levelname", "levelno", "msecs", "message",
            "taskName",
        }
        for key, value in record.__dict__.items():
            if key not in standard_attrs and not key.startswith("_"):
                log_entry[key] = value

        return json.dumps(log_entry, default=str)


def create_logger(
    name: str,
    log_path: str,
    app_name: str,
    level: str = "INFO",
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
) -> logging.Logger:
    """Create a structured JSON logger with file and console handlers.

    Args:
        name: Logger name.
        log_path: DBFS path for log files.
        app_name: Application identifier for log entries.
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        max_bytes: Max log file size before rotation.
        backup_count: Number of rotated log files to retain.

    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Avoid duplicate handlers on re-execution
    if logger.handlers:
        logger.handlers.clear()

    formatter = StructuredJsonFormatter(app_name=app_name)

    # File handler: write to DBFS
    os.makedirs(log_path, exist_ok=True)
    log_file = os.path.join(log_path, f"{app_name}.log")
    file_handler = RotatingFileHandler(
        log_file, maxBytes=max_bytes, backupCount=backup_count
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler: also output to notebook
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

# COMMAND ----------

# Initialize the logger
logger = create_logger(
    name="observability",
    log_path=LOG_PATH,
    app_name=APP_NAME,
    level=LOG_LEVEL,
)

logger.info("Observability logger initialized", extra={"log_path": LOG_PATH})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Contextual Logging Utilities

# COMMAND ----------

import functools
import time
from contextlib import contextmanager


@contextmanager
def log_operation(operation_name: str, **extra_context):
    """Context manager that logs the start, end, and duration of an operation.

    Usage:
        with log_operation("load_table", table="my_catalog.my_schema.my_table"):
            df = spark.table("my_catalog.my_schema.my_table")
    """
    start_time = time.time()
    context = {"operation": operation_name, **extra_context}

    logger.info(f"Starting: {operation_name}", extra=context)
    try:
        yield
        duration = time.time() - start_time
        logger.info(
            f"Completed: {operation_name}",
            extra={**context, "duration_seconds": round(duration, 3), "status": "success"},
        )
    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            f"Failed: {operation_name}",
            extra={**context, "duration_seconds": round(duration, 3), "status": "error"},
            exc_info=True,
        )
        raise


def log_transform(func):
    """Decorator that wraps a transformation function with structured logging.

    Usage:
        @log_transform
        def clean_data(df):
            return df.dropna()
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        func_name = func.__name__
        start_time = time.time()
        logger.info(f"Transform started: {func_name}", extra={"transform": func_name})
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            logger.info(
                f"Transform completed: {func_name}",
                extra={
                    "transform": func_name,
                    "duration_seconds": round(duration, 3),
                    "status": "success",
                },
            )
            return result
        except Exception as e:
            duration = time.time() - start_time
            logger.error(
                f"Transform failed: {func_name}",
                extra={
                    "transform": func_name,
                    "duration_seconds": round(duration, 3),
                    "status": "error",
                },
                exc_info=True,
            )
            raise

    return wrapper

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Example: Transformation with Error Handling

# COMMAND ----------

@log_transform
def example_data_validation(df, required_columns):
    """Validate that a DataFrame contains the required columns and has no nulls in key fields."""
    missing_cols = [c for c in required_columns if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Check for nulls in required columns
    from pyspark.sql import functions as F

    null_counts = {}
    for col_name in required_columns:
        null_count = df.where(F.col(col_name).isNull()).count()
        if null_count > 0:
            null_counts[col_name] = null_count

    if null_counts:
        logger.warning(
            "Null values detected in required columns",
            extra={"null_counts": null_counts},
        )

    return df


@log_transform
def example_enrichment(df, lookup_table_name):
    """Example enrichment join with error handling."""
    try:
        lookup_df = spark.table(lookup_table_name)
    except Exception:
        logger.error(
            f"Lookup table not found: {lookup_table_name}",
            extra={"lookup_table": lookup_table_name},
            exc_info=True,
        )
        raise

    return df.join(lookup_df, on="id", how="left")

# COMMAND ----------

# Demonstrate the logging framework with a sample pipeline
logger.info("Starting example pipeline run")

try:
    with log_operation("create_sample_data"):
        sample_df = spark.createDataFrame(
            [(1, "alice", 100), (2, "bob", None), (3, None, 300)],
            ["id", "name", "value"],
        )
        logger.info(
            "Sample data created",
            extra={"row_count": sample_df.count(), "columns": sample_df.columns},
        )

    with log_operation("validate_sample_data", required_columns=["id", "name"]):
        validated_df = example_data_validation(sample_df, required_columns=["id", "name"])

    logger.info("Example pipeline completed successfully")

except Exception as e:
    logger.critical(
        "Pipeline failed",
        extra={"error_type": type(e).__name__},
        exc_info=True,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Log Querying and Analysis
# MAGIC
# MAGIC Read the structured JSON logs back from DBFS for analysis and alerting.

# COMMAND ----------

import os

log_file = os.path.join(LOG_PATH, f"{APP_NAME}.log")

if os.path.exists(log_file):
    # Read the JSON logs as a DataFrame for analysis
    logs_df = spark.read.json(f"file:{log_file}")

    display(logs_df.orderBy("timestamp", ascending=False))
else:
    print(f"No log file found at {log_file}. Run the example pipeline above first.")

# COMMAND ----------

if os.path.exists(log_file):
    # Summary of log levels
    from pyspark.sql import functions as F

    log_summary = (
        logs_df.groupBy("level")
        .agg(
            F.count("*").alias("count"),
            F.min("timestamp").alias("earliest"),
            F.max("timestamp").alias("latest"),
        )
        .orderBy("count", ascending=False)
    )

    display(log_summary)

# COMMAND ----------

if os.path.exists(log_file):
    # Error and critical log entries for alerting
    error_logs = logs_df.where(F.col("level").isin("ERROR", "CRITICAL"))
    display(error_logs.orderBy("timestamp", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Alerting Framework
# MAGIC
# MAGIC Create alert-ready views from the structured logs for integration with
# MAGIC Databricks SQL Alerts or external webhook notifications.

# COMMAND ----------

if os.path.exists(log_file):
    logs_df.createOrReplaceTempView("application_logs")

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW log_alerts AS
        SELECT
            level,
            COUNT(*) AS count,
            MAX(timestamp) AS latest_occurrence,
            COLLECT_SET(message) AS unique_messages,
            CASE
                WHEN level = 'CRITICAL' THEN 'PAGE'
                WHEN level = 'ERROR' AND COUNT(*) > 5 THEN 'ALERT'
                WHEN level = 'ERROR' THEN 'WARN'
                ELSE 'OK'
            END AS alert_action
        FROM application_logs
        WHERE level IN ('ERROR', 'CRITICAL')
          AND timestamp >= DATE_FORMAT(CURRENT_TIMESTAMP() - INTERVAL 1 HOUR, "yyyy-MM-dd'T'HH:mm:ss")
        GROUP BY level
    """)

    display(spark.sql("SELECT * FROM log_alerts"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Security: PII Protection
# MAGIC
# MAGIC Utility to sanitize log messages and prevent PII leakage.

# COMMAND ----------

import re

# Patterns to redact from log messages
PII_PATTERNS = {
    "email": re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"),
    "ssn": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "phone": re.compile(r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b"),
    "credit_card": re.compile(r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b"),
}


def sanitize_message(message: str) -> str:
    """Redact PII patterns from log messages."""
    for pii_type, pattern in PII_PATTERNS.items():
        message = pattern.sub(f"[REDACTED_{pii_type.upper()}]", message)
    return message


class PiiSafeFormatter(StructuredJsonFormatter):
    """Extends StructuredJsonFormatter with automatic PII redaction."""

    def format(self, record: logging.LogRecord) -> str:
        record.msg = sanitize_message(str(record.msg))
        return super().format(record)


# Example: create a PII-safe logger
def create_pii_safe_logger(name: str, log_path: str, app_name: str, level: str = "INFO"):
    """Create a logger with automatic PII redaction."""
    safe_logger = logging.getLogger(f"{name}_pii_safe")
    safe_logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    if safe_logger.handlers:
        safe_logger.handlers.clear()

    formatter = PiiSafeFormatter(app_name=app_name)

    os.makedirs(log_path, exist_ok=True)
    log_file = os.path.join(log_path, f"{app_name}_safe.log")
    file_handler = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5)
    file_handler.setFormatter(formatter)
    safe_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    safe_logger.addHandler(console_handler)

    return safe_logger


# Demonstrate PII redaction
safe_logger = create_pii_safe_logger("observability", LOG_PATH, APP_NAME, LOG_LEVEL)
safe_logger.info("User email: john.doe@example.com, SSN: 123-45-6789")
safe_logger.info("No PII in this message - all clean")
