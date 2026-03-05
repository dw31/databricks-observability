# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 4: Streaming and Pipeline Performance
# MAGIC
# MAGIC Ensure streaming applications maintain data freshness and high throughput without
# MAGIC falling behind. Monitor batch durations, throughput, backlogs, and resource utilization.
# MAGIC
# MAGIC **Data Sources:**
# MAGIC - DLT `event_log` (filtering by `event_type = 'flow_progress'`)
# MAGIC - `StreamingQueryListener` metrics
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Unity Catalog enabled
# MAGIC - Active DLT pipelines or Structured Streaming jobs
# MAGIC - StreamingQueryListener configured (for custom streaming metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("pipeline_id", "", "DLT Pipeline ID")
dbutils.widgets.text("streaming_metrics_table", "", "StreamingQueryListener Metrics Table (optional)")

PIPELINE_ID = dbutils.widgets.get("pipeline_id")
STREAMING_METRICS_TABLE = dbutils.widgets.get("streaming_metrics_table")

LOOKBACK_DAYS = 7

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Throughput and Duration (DLT Flow Progress)
# MAGIC
# MAGIC Calculate average batch duration and throughput in rows per second from
# MAGIC DLT flow progress events.

# COMMAND ----------

from pyspark.sql import functions as F

if PIPELINE_ID:
    flow_progress_df = spark.sql(f"""
        SELECT
            timestamp,
            origin.flow_name,
            origin.dataset_name,
            details:flow_progress:num_output_rows AS num_output_rows,
            details:flow_progress:metrics:num_output_rows AS metric_output_rows,
            details:flow_progress:status AS flow_status,
            details:flow_progress:data_quality AS data_quality
        FROM event_log('{PIPELINE_ID}')
        WHERE event_type = 'flow_progress'
          AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL {LOOKBACK_DAYS} DAYS
        ORDER BY timestamp DESC
    """)

    display(flow_progress_df)

# COMMAND ----------

if PIPELINE_ID:
    throughput_df = spark.sql(f"""
        WITH flow_metrics AS (
            SELECT
                origin.flow_name,
                origin.dataset_name,
                timestamp,
                CAST(details:flow_progress:num_output_rows AS BIGINT) AS output_rows,
                LAG(timestamp) OVER (
                    PARTITION BY origin.flow_name
                    ORDER BY timestamp
                ) AS prev_timestamp
            FROM event_log('{PIPELINE_ID}')
            WHERE event_type = 'flow_progress'
              AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL {LOOKBACK_DAYS} DAYS
        ),
        with_duration AS (
            SELECT
                flow_name,
                dataset_name,
                timestamp,
                output_rows,
                TIMESTAMPDIFF(SECOND, prev_timestamp, timestamp) AS batch_duration_seconds
            FROM flow_metrics
            WHERE prev_timestamp IS NOT NULL
        )
        SELECT
            flow_name,
            dataset_name,
            COUNT(*) AS total_batches,
            ROUND(AVG(batch_duration_seconds), 2) AS avg_batch_duration_sec,
            ROUND(MIN(batch_duration_seconds), 2) AS min_batch_duration_sec,
            ROUND(MAX(batch_duration_seconds), 2) AS max_batch_duration_sec,
            ROUND(PERCENTILE(batch_duration_seconds, 0.95), 2) AS p95_batch_duration_sec,
            SUM(output_rows) AS total_rows_processed,
            ROUND(
                SUM(output_rows) / NULLIF(SUM(batch_duration_seconds), 0), 2
            ) AS avg_rows_per_second
        FROM with_duration
        WHERE batch_duration_seconds > 0
        GROUP BY flow_name, dataset_name
        ORDER BY avg_rows_per_second DESC
    """)

    display(throughput_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Backlog Monitoring
# MAGIC
# MAGIC Track `backlog_bytes` and `backlog_files` to proactively tune Kafka/Kinesis offsets
# MAGIC or Auto Loader instances before downstream systems are impacted.

# COMMAND ----------

if PIPELINE_ID:
    backlog_df = spark.sql(f"""
        SELECT
            timestamp,
            origin.flow_name,
            origin.dataset_name,
            CAST(details:flow_progress:metrics:backlog_bytes AS BIGINT) AS backlog_bytes,
            CAST(details:flow_progress:metrics:backlog_files AS BIGINT) AS backlog_files,
            CAST(details:flow_progress:metrics:num_output_rows AS BIGINT) AS output_rows
        FROM event_log('{PIPELINE_ID}')
        WHERE event_type = 'flow_progress'
          AND (
              details:flow_progress:metrics:backlog_bytes IS NOT NULL
              OR details:flow_progress:metrics:backlog_files IS NOT NULL
          )
          AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL {LOOKBACK_DAYS} DAYS
        ORDER BY timestamp DESC
    """)

    display(backlog_df)

# COMMAND ----------

if PIPELINE_ID:
    # Backlog trend analysis: detect growing backlogs
    backlog_trend_df = spark.sql(f"""
        WITH backlog_series AS (
            SELECT
                origin.flow_name,
                DATE(timestamp) AS log_date,
                AVG(CAST(details:flow_progress:metrics:backlog_bytes AS BIGINT)) AS avg_backlog_bytes,
                MAX(CAST(details:flow_progress:metrics:backlog_bytes AS BIGINT)) AS max_backlog_bytes,
                AVG(CAST(details:flow_progress:metrics:backlog_files AS BIGINT)) AS avg_backlog_files,
                MAX(CAST(details:flow_progress:metrics:backlog_files AS BIGINT)) AS max_backlog_files
            FROM event_log('{PIPELINE_ID}')
            WHERE event_type = 'flow_progress'
              AND details:flow_progress:metrics:backlog_bytes IS NOT NULL
              AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL {LOOKBACK_DAYS} DAYS
            GROUP BY origin.flow_name, DATE(timestamp)
        )
        SELECT
            flow_name,
            log_date,
            avg_backlog_bytes,
            max_backlog_bytes,
            avg_backlog_files,
            max_backlog_files,
            LAG(avg_backlog_bytes) OVER (PARTITION BY flow_name ORDER BY log_date) AS prev_day_avg_bytes,
            CASE
                WHEN LAG(avg_backlog_bytes) OVER (PARTITION BY flow_name ORDER BY log_date) > 0
                    THEN ROUND(
                        (avg_backlog_bytes - LAG(avg_backlog_bytes) OVER (PARTITION BY flow_name ORDER BY log_date))
                        / LAG(avg_backlog_bytes) OVER (PARTITION BY flow_name ORDER BY log_date) * 100, 2
                    )
                ELSE NULL
            END AS backlog_growth_pct
        FROM backlog_series
        ORDER BY flow_name, log_date DESC
    """)

    display(backlog_trend_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Resource Utilization
# MAGIC
# MAGIC Monitor average task slots and queued tasks to optimize compute scaling.

# COMMAND ----------

if PIPELINE_ID:
    resource_df = spark.sql(f"""
        SELECT
            timestamp,
            origin.flow_name,
            CAST(details:flow_progress:metrics:num_task_slots AS INT) AS num_task_slots,
            CAST(details:flow_progress:metrics:num_queued_tasks AS INT) AS num_queued_tasks,
            CAST(details:flow_progress:metrics:num_active_tasks AS INT) AS num_active_tasks
        FROM event_log('{PIPELINE_ID}')
        WHERE event_type = 'flow_progress'
          AND (
              details:flow_progress:metrics:num_task_slots IS NOT NULL
              OR details:flow_progress:metrics:num_queued_tasks IS NOT NULL
          )
          AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL {LOOKBACK_DAYS} DAYS
        ORDER BY timestamp DESC
    """)

    display(resource_df)

# COMMAND ----------

if PIPELINE_ID:
    # Resource utilization summary
    resource_summary_df = spark.sql(f"""
        SELECT
            origin.flow_name,
            ROUND(AVG(CAST(details:flow_progress:metrics:num_task_slots AS INT)), 2) AS avg_task_slots,
            ROUND(AVG(CAST(details:flow_progress:metrics:num_queued_tasks AS INT)), 2) AS avg_queued_tasks,
            ROUND(AVG(CAST(details:flow_progress:metrics:num_active_tasks AS INT)), 2) AS avg_active_tasks,
            MAX(CAST(details:flow_progress:metrics:num_queued_tasks AS INT)) AS max_queued_tasks,
            ROUND(
                AVG(CAST(details:flow_progress:metrics:num_active_tasks AS INT))
                / NULLIF(AVG(CAST(details:flow_progress:metrics:num_task_slots AS INT)), 0) * 100, 2
            ) AS avg_utilization_pct
        FROM event_log('{PIPELINE_ID}')
        WHERE event_type = 'flow_progress'
          AND details:flow_progress:metrics:num_task_slots IS NOT NULL
          AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL {LOOKBACK_DAYS} DAYS
        GROUP BY origin.flow_name
        ORDER BY avg_utilization_pct DESC
    """)

    display(resource_summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. StreamingQueryListener Metrics (Optional)
# MAGIC
# MAGIC If a custom `StreamingQueryListener` is configured to write metrics to a Delta table,
# MAGIC query it for additional streaming insights.

# COMMAND ----------

if STREAMING_METRICS_TABLE:
    listener_df = spark.sql(f"""
        SELECT
            query_name,
            batch_id,
            timestamp,
            num_input_rows,
            input_rows_per_second,
            processed_rows_per_second,
            batch_duration_ms,
            ROUND(batch_duration_ms / 1000.0, 2) AS batch_duration_sec
        FROM {STREAMING_METRICS_TABLE}
        WHERE timestamp >= CURRENT_TIMESTAMP() - INTERVAL {LOOKBACK_DAYS} DAYS
        ORDER BY timestamp DESC
    """)

    display(listener_df)

    # Summary statistics per query
    listener_summary_df = spark.sql(f"""
        SELECT
            query_name,
            COUNT(*) AS total_batches,
            ROUND(AVG(batch_duration_ms) / 1000.0, 2) AS avg_batch_duration_sec,
            ROUND(AVG(input_rows_per_second), 2) AS avg_input_rows_per_sec,
            ROUND(AVG(processed_rows_per_second), 2) AS avg_processed_rows_per_sec,
            SUM(num_input_rows) AS total_input_rows,
            ROUND(PERCENTILE(batch_duration_ms, 0.95) / 1000.0, 2) AS p95_batch_duration_sec
        FROM {STREAMING_METRICS_TABLE}
        WHERE timestamp >= CURRENT_TIMESTAMP() - INTERVAL {LOOKBACK_DAYS} DAYS
        GROUP BY query_name
        ORDER BY avg_batch_duration_sec DESC
    """)

    display(listener_summary_df)
else:
    print("INFO: No streaming_metrics_table provided. Skipping StreamingQueryListener metrics.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Alerting Integration
# MAGIC
# MAGIC Summary view for SQL Alerts on streaming health.

# COMMAND ----------

if PIPELINE_ID:
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW streaming_alerts AS
        WITH recent_backlogs AS (
            SELECT
                origin.flow_name,
                AVG(CAST(details:flow_progress:metrics:backlog_bytes AS BIGINT)) AS avg_backlog_bytes,
                MAX(CAST(details:flow_progress:metrics:backlog_bytes AS BIGINT)) AS max_backlog_bytes
            FROM event_log('{PIPELINE_ID}')
            WHERE event_type = 'flow_progress'
              AND details:flow_progress:metrics:backlog_bytes IS NOT NULL
              AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 DAY
            GROUP BY origin.flow_name
        ),
        recent_utilization AS (
            SELECT
                origin.flow_name,
                AVG(CAST(details:flow_progress:metrics:num_queued_tasks AS INT)) AS avg_queued,
                AVG(CAST(details:flow_progress:metrics:num_active_tasks AS INT))
                    / NULLIF(AVG(CAST(details:flow_progress:metrics:num_task_slots AS INT)), 0) * 100 AS utilization_pct
            FROM event_log('{PIPELINE_ID}')
            WHERE event_type = 'flow_progress'
              AND details:flow_progress:metrics:num_task_slots IS NOT NULL
              AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 DAY
            GROUP BY origin.flow_name
        )
        SELECT
            COALESCE(b.flow_name, u.flow_name) AS flow_name,
            b.avg_backlog_bytes,
            b.max_backlog_bytes,
            ROUND(u.utilization_pct, 2) AS utilization_pct,
            ROUND(u.avg_queued, 2) AS avg_queued_tasks,
            CASE
                WHEN b.max_backlog_bytes > 1073741824 THEN 'CRITICAL'
                WHEN b.max_backlog_bytes > 104857600 THEN 'WARNING'
                ELSE 'OK'
            END AS backlog_alert,
            CASE
                WHEN u.utilization_pct > 90 THEN 'WARNING'
                ELSE 'OK'
            END AS utilization_alert
        FROM recent_backlogs b
        FULL OUTER JOIN recent_utilization u
          ON b.flow_name = u.flow_name
    """)

    display(spark.sql("SELECT * FROM streaming_alerts"))
