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
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType

# Define the schema for the expectations JSON array
expectations_schema = ArrayType(StructType([
    StructField("name", StringType(), True),
    StructField("dataset", StringType(), True),
    StructField("passed_records", LongType(), True),
    StructField("failed_records", LongType(), True),
]))

# Query the DLT event log for expectation results
if PIPELINE_ID:
    event_log_df = (
        spark.sql(f"SELECT * FROM event_log('{PIPELINE_ID}')")
        .filter(F.col("event_type") == "flow_progress")
        .filter(F.col("timestamp") >= F.current_timestamp() - F.expr(f"INTERVAL {LOOKBACK_DAYS} DAYS"))
        .select(
            F.col("id"),
            F.col("timestamp"),
            F.col("origin.pipeline_id").alias("pipeline_id"),
            F.col("origin.flow_name").alias("flow_name"),
            F.col("origin.dataset_name").alias("dataset_name"),
            F.col("event_type"),
            F.col("details"),
        )
    )
else:
    print("WARNING: No pipeline_id provided. Set the widget value to query DLT event logs.")
    event_log_df = spark.createDataFrame(
        [],
        schema="id STRING, timestamp TIMESTAMP, pipeline_id STRING, flow_name STRING, dataset_name STRING, event_type STRING, details STRING",
    )

# COMMAND ----------

# Parse the JSON details to extract expectation metrics
if PIPELINE_ID:
    raw_events = (
        spark.sql(f"SELECT * FROM event_log('{PIPELINE_ID}')")
        .filter(F.col("event_type") == "flow_progress")
        .filter(F.col("details:flow_progress:data_quality").isNotNull())
        .filter(F.col("timestamp") >= F.current_timestamp() - F.expr(f"INTERVAL {LOOKBACK_DAYS} DAYS"))
        .select(
            F.col("timestamp"),
            F.col("origin.flow_name").alias("flow_name"),
            F.col("origin.dataset_name").alias("dataset_name"),
            F.col("details:flow_progress:data_quality:expectations").alias("expectations_json"),
        )
    )

    expectations_df = (
        raw_events
        .withColumn("expectations", F.from_json(F.col("expectations_json"), expectations_schema))
        .withColumn("exp", F.explode("expectations"))
        .select(
            F.col("timestamp"),
            F.col("flow_name"),
            F.col("dataset_name"),
            F.col("exp.name").alias("expectation_name"),
            F.col("exp.passed_records").alias("passed_records"),
            F.col("exp.failed_records").alias("failed_records"),
        )
        .withColumn("total_records", F.col("passed_records") + F.col("failed_records"))
        .withColumn(
            "pass_rate_pct",
            F.round(
                F.col("passed_records") * 100.0 / F.when(
                    F.col("total_records") != 0, F.col("total_records")
                ),
                2,
            ),
        )
        .orderBy(F.col("timestamp").desc())
    )

    display(expectations_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Quality Summaries
# MAGIC
# MAGIC Calculate the average pass and fail rates of data expectations across the pipeline.

# COMMAND ----------

if PIPELINE_ID:
    raw_events_summary = (
        spark.sql(f"SELECT * FROM event_log('{PIPELINE_ID}')")
        .filter(F.col("event_type") == "flow_progress")
        .filter(F.col("details:flow_progress:data_quality").isNotNull())
        .filter(F.col("timestamp") >= F.current_timestamp() - F.expr(f"INTERVAL {LOOKBACK_DAYS} DAYS"))
        .select(
            F.col("origin.flow_name").alias("flow_name"),
            F.col("origin.dataset_name").alias("dataset_name"),
            F.col("details:flow_progress:data_quality:expectations").alias("expectations_json"),
        )
    )

    exploded_summary = (
        raw_events_summary
        .withColumn("expectations", F.from_json(F.col("expectations_json"), expectations_schema))
        .withColumn("exp", F.explode("expectations"))
        .select(
            F.col("flow_name"),
            F.col("dataset_name"),
            F.col("exp.name").alias("expectation_name"),
            F.col("exp.passed_records").alias("passed_records"),
            F.col("exp.failed_records").alias("failed_records"),
        )
        .withColumn("total_records", F.col("passed_records") + F.col("failed_records"))
        .withColumn(
            "pass_rate_pct",
            F.col("passed_records") * 100.0 / F.when(
                F.col("total_records") != 0, F.col("total_records")
            ),
        )
        .withColumn(
            "fail_rate_pct",
            F.col("failed_records") * 100.0 / F.when(
                F.col("total_records") != 0, F.col("total_records")
            ),
        )
    )

    quality_summary_df = (
        exploded_summary
        .groupBy("dataset_name", "expectation_name")
        .agg(
            F.count("*").alias("num_evaluations"),
            F.sum("passed_records").alias("total_passed"),
            F.sum("failed_records").alias("total_failed"),
            (F.sum("passed_records") + F.sum("failed_records")).alias("total_records"),
            F.round(F.avg("pass_rate_pct"), 2).alias("avg_pass_rate_pct"),
            F.round(F.avg("fail_rate_pct"), 2).alias("avg_fail_rate_pct"),
            F.min("pass_rate_pct").alias("min_pass_rate_pct"),
        )
        .orderBy(F.col("avg_fail_rate_pct").desc())
    )

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

    profile_df = (
        spark.table(profile_table)
        .filter(F.col("window_end") >= F.current_timestamp() - F.expr(f"INTERVAL {LOOKBACK_DAYS} DAYS"))
        .withColumn(
            "null_pct",
            F.col("num_nulls") * 100.0 / F.when(F.col("row_count") != 0, F.col("row_count")),
        )
        .select(
            "column_name",
            "data_type",
            "window_start",
            "window_end",
            "min",
            "max",
            "mean",
            "stddev",
            "num_nulls",
            "null_pct",
            "distinct_count",
            "row_count",
        )
        .orderBy(F.col("window_end").desc(), F.col("column_name"))
    )

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

    drift_df = (
        spark.table(drift_table)
        .filter(F.col("window_end") >= F.current_timestamp() - F.expr(f"INTERVAL {LOOKBACK_DAYS} DAYS"))
        .withColumn("current_value", F.col("value"))
        .withColumn("absolute_drift", F.abs(F.col("value") - F.col("baseline_value")))
        .withColumn(
            "drift_pct",
            F.when(
                F.col("baseline_value") != 0,
                F.round(F.abs(F.col("value") - F.col("baseline_value")) / F.abs(F.col("baseline_value")) * 100, 2),
            ),
        )
        .select(
            "column_name",
            "window_start",
            "window_end",
            "drift_type",
            "statistic",
            "current_value",
            "baseline_value",
            "absolute_drift",
            "drift_pct",
        )
        .orderBy(F.col("drift_pct").desc_nulls_last())
    )

    display(drift_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Alerting Integration
# MAGIC
# MAGIC Summary view for SQL Alerts on quality breaches and drift.

# COMMAND ----------

if PIPELINE_ID:
    raw_events_alerts = (
        spark.sql(f"SELECT * FROM event_log('{PIPELINE_ID}')")
        .filter(F.col("event_type") == "flow_progress")
        .filter(F.col("details:flow_progress:data_quality").isNotNull())
        .filter(F.col("timestamp") >= F.current_timestamp() - F.expr("INTERVAL 1 DAY"))
        .select(
            F.col("origin.dataset_name").alias("dataset_name"),
            F.col("details:flow_progress:data_quality:expectations").alias("expectations_json"),
        )
    )

    exploded_alerts = (
        raw_events_alerts
        .withColumn("expectations", F.from_json(F.col("expectations_json"), expectations_schema))
        .withColumn("exp", F.explode("expectations"))
        .select(
            F.col("dataset_name"),
            F.col("exp.name").alias("expectation_name"),
            F.col("exp.passed_records").alias("passed_records"),
            F.col("exp.failed_records").alias("failed_records"),
        )
        .withColumn("total_records", F.col("passed_records") + F.col("failed_records"))
        .withColumn(
            "fail_rate_pct",
            F.col("failed_records") * 100.0 / F.when(
                F.col("total_records") != 0, F.col("total_records")
            ),
        )
    )

    quality_alerts_df = (
        exploded_alerts
        .groupBy("dataset_name", "expectation_name")
        .agg(
            F.sum("failed_records").alias("total_failed_records"),
            F.round(F.avg("fail_rate_pct"), 2).alias("avg_fail_rate_pct"),
        )
        .filter(F.col("total_failed_records") > 0)
        .withColumn(
            "quality_alert_status",
            F.when(F.col("avg_fail_rate_pct") > 5.0, F.lit("ALERT")).otherwise(F.lit("OK")),
        )
    )

    quality_alerts_df.createOrReplaceTempView("quality_alerts")

    display(quality_alerts_df.orderBy(F.col("avg_fail_rate_pct").desc()))
