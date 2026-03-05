# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 2: Data Quality and Lakehouse Monitoring
# MAGIC
# MAGIC Measure statistical distribution, track data drift, and monitor validation constraints
# MAGIC (expectations) at runtime using DLT event logs and Lakehouse Monitoring metric tables.
# MAGIC
# MAGIC **Data Sources:**
# MAGIC - Delta Live Tables (DLT) event logs via `event_log()` TVF
# MAGIC - Lakehouse Monitoring metric tables
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Unity Catalog enabled
# MAGIC - Active DLT pipelines with expectations defined
# MAGIC - Lakehouse Monitoring configured on target tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# DBTITLE 1,Pipeline and monitoring configuration
# DLT Pipeline ID to monitor (set via widget or hardcode)
dbutils.widgets.text("pipeline_id", "", "DLT Pipeline ID")
dbutils.widgets.text("monitoring_catalog", "main", "Monitoring Catalog")
dbutils.widgets.text("monitoring_schema", "default", "Monitoring Schema")
dbutils.widgets.text("monitored_table", "", "Monitored Table Name")

PIPELINE_ID = dbutils.widgets.get("pipeline_id")
MONITORING_CATALOG = dbutils.widgets.get("monitoring_catalog")
MONITORING_SCHEMA = dbutils.widgets.get("monitoring_schema")
MONITORED_TABLE = dbutils.widgets.get("monitored_table")

LOOKBACK_DAYS = 30

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Expectation Tracking
# MAGIC
# MAGIC Query the DLT `event_log()` table-valued function to extract `passed_records` and
# MAGIC `failed_records` for data quality rules.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, MapType

# Query the DLT event log for expectation results
if PIPELINE_ID:
    event_log_df = spark.sql(f"""
        SELECT
            id,
            timestamp,
            origin.pipeline_id,
            origin.flow_name,
            origin.dataset_name,
            event_type,
            details
        FROM event_log('{PIPELINE_ID}')
        WHERE event_type = 'flow_progress'
          AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL {LOOKBACK_DAYS} DAYS
    """)
else:
    print("WARNING: No pipeline_id provided. Set the widget value to query DLT event logs.")
    event_log_df = spark.createDataFrame([], schema="id STRING, timestamp TIMESTAMP, pipeline_id STRING, flow_name STRING, dataset_name STRING, event_type STRING, details STRING")

# COMMAND ----------

# Parse the JSON details to extract expectation metrics
if PIPELINE_ID:
    expectations_df = spark.sql(f"""
        WITH parsed_events AS (
            SELECT
                timestamp,
                origin.flow_name,
                origin.dataset_name,
                details:flow_progress:data_quality:expectations AS expectations_json
            FROM event_log('{PIPELINE_ID}')
            WHERE event_type = 'flow_progress'
              AND details:flow_progress:data_quality IS NOT NULL
              AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL {LOOKBACK_DAYS} DAYS
        ),
        exploded AS (
            SELECT
                timestamp,
                flow_name,
                dataset_name,
                exp.name AS expectation_name,
                exp.dataset AS expectation_dataset,
                exp.passed_records,
                exp.failed_records
            FROM parsed_events
            LATERAL VIEW EXPLODE(
                FROM_JSON(expectations_json, 'ARRAY<STRUCT<name: STRING, dataset: STRING, passed_records: BIGINT, failed_records: BIGINT>>')
            ) AS exp
        )
        SELECT
            timestamp,
            flow_name,
            dataset_name,
            expectation_name,
            passed_records,
            failed_records,
            passed_records + failed_records AS total_records,
            ROUND(passed_records * 100.0 / NULLIF(passed_records + failed_records, 0), 2) AS pass_rate_pct
        FROM exploded
        ORDER BY timestamp DESC
    """)

    display(expectations_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Quality Summaries
# MAGIC
# MAGIC Calculate the average pass and fail rates of data expectations across the pipeline.

# COMMAND ----------

if PIPELINE_ID:
    quality_summary_df = spark.sql(f"""
        WITH parsed_events AS (
            SELECT
                origin.flow_name,
                origin.dataset_name,
                details:flow_progress:data_quality:expectations AS expectations_json
            FROM event_log('{PIPELINE_ID}')
            WHERE event_type = 'flow_progress'
              AND details:flow_progress:data_quality IS NOT NULL
              AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL {LOOKBACK_DAYS} DAYS
        ),
        exploded AS (
            SELECT
                flow_name,
                dataset_name,
                exp.name AS expectation_name,
                exp.passed_records,
                exp.failed_records
            FROM parsed_events
            LATERAL VIEW EXPLODE(
                FROM_JSON(expectations_json, 'ARRAY<STRUCT<name: STRING, dataset: STRING, passed_records: BIGINT, failed_records: BIGINT>>')
            ) AS exp
        )
        SELECT
            dataset_name,
            expectation_name,
            COUNT(*) AS num_evaluations,
            SUM(passed_records) AS total_passed,
            SUM(failed_records) AS total_failed,
            SUM(passed_records) + SUM(failed_records) AS total_records,
            ROUND(
                AVG(passed_records * 100.0 / NULLIF(passed_records + failed_records, 0)), 2
            ) AS avg_pass_rate_pct,
            ROUND(
                AVG(failed_records * 100.0 / NULLIF(passed_records + failed_records, 0)), 2
            ) AS avg_fail_rate_pct,
            MIN(passed_records * 100.0 / NULLIF(passed_records + failed_records, 0)) AS min_pass_rate_pct
        FROM exploded
        GROUP BY dataset_name, expectation_name
        ORDER BY avg_fail_rate_pct DESC
    """)

    display(quality_summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Statistical Profiling
# MAGIC
# MAGIC Use Lakehouse Monitoring profile metrics to capture min, max, mean, stddev,
# MAGIC and missing value counts for critical columns.

# COMMAND ----------

if MONITORED_TABLE:
    # Lakehouse Monitoring stores profile metrics in a system-generated table
    profile_table = f"{MONITORING_CATALOG}.{MONITORING_SCHEMA}.{MONITORED_TABLE}_profile_metrics"

    profile_df = spark.sql(f"""
        SELECT
            column_name,
            data_type,
            window_start,
            window_end,
            min,
            max,
            mean,
            stddev,
            num_nulls,
            num_nulls * 100.0 / NULLIF(row_count, 0) AS null_pct,
            distinct_count,
            row_count
        FROM {profile_table}
        WHERE window_end >= CURRENT_TIMESTAMP() - INTERVAL {LOOKBACK_DAYS} DAYS
        ORDER BY window_end DESC, column_name
    """)

    display(profile_df)
else:
    print("WARNING: No monitored_table provided. Set the widget value to query profile metrics.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Drift Detection
# MAGIC
# MAGIC Compare current profile metrics against baseline to detect drift.

# COMMAND ----------

if MONITORED_TABLE:
    drift_table = f"{MONITORING_CATALOG}.{MONITORING_SCHEMA}.{MONITORED_TABLE}_drift_metrics"

    drift_df = spark.sql(f"""
        SELECT
            column_name,
            window_start,
            window_end,
            drift_type,
            statistic,
            value AS current_value,
            baseline_value,
            ABS(value - baseline_value) AS absolute_drift,
            CASE
                WHEN baseline_value != 0
                    THEN ROUND(ABS(value - baseline_value) / ABS(baseline_value) * 100, 2)
                ELSE NULL
            END AS drift_pct
        FROM {drift_table}
        WHERE window_end >= CURRENT_TIMESTAMP() - INTERVAL {LOOKBACK_DAYS} DAYS
        ORDER BY drift_pct DESC NULLS LAST
    """)

    display(drift_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Alerting Integration
# MAGIC
# MAGIC Summary view for SQL Alerts on quality breaches and drift.

# COMMAND ----------

if PIPELINE_ID:
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW quality_alerts AS
        WITH parsed_events AS (
            SELECT
                origin.dataset_name,
                details:flow_progress:data_quality:expectations AS expectations_json
            FROM event_log('{PIPELINE_ID}')
            WHERE event_type = 'flow_progress'
              AND details:flow_progress:data_quality IS NOT NULL
              AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 DAY
        ),
        exploded AS (
            SELECT
                dataset_name,
                exp.name AS expectation_name,
                exp.passed_records,
                exp.failed_records
            FROM parsed_events
            LATERAL VIEW EXPLODE(
                FROM_JSON(expectations_json, 'ARRAY<STRUCT<name: STRING, dataset: STRING, passed_records: BIGINT, failed_records: BIGINT>>')
            ) AS exp
        )
        SELECT
            dataset_name,
            expectation_name,
            SUM(failed_records) AS total_failed_records,
            ROUND(
                AVG(failed_records * 100.0 / NULLIF(passed_records + failed_records, 0)), 2
            ) AS avg_fail_rate_pct,
            CASE
                WHEN AVG(failed_records * 100.0 / NULLIF(passed_records + failed_records, 0)) > 5.0
                    THEN 'ALERT'
                ELSE 'OK'
            END AS quality_alert_status
        FROM exploded
        GROUP BY dataset_name, expectation_name
        HAVING SUM(failed_records) > 0
    """)

    display(spark.sql("SELECT * FROM quality_alerts ORDER BY avg_fail_rate_pct DESC"))
