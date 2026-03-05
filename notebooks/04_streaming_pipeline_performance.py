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
from pyspark.sql.window import Window

if PIPELINE_ID:
    # Load raw event_log via TVF, then filter/transform with PySpark
    raw_events_df = spark.sql(f"SELECT * FROM event_log('{PIPELINE_ID}')")

    flow_progress_df = (
        raw_events_df
        .filter(F.col("event_type") == "flow_progress")
        .filter(F.col("timestamp") >= F.current_timestamp() - F.expr(f"INTERVAL {LOOKBACK_DAYS} DAYS"))
        .select(
            F.col("timestamp"),
            F.col("origin.flow_name").alias("flow_name"),
            F.col("origin.dataset_name").alias("dataset_name"),
            F.col("details:flow_progress:num_output_rows").alias("num_output_rows"),
            F.col("details:flow_progress:metrics:num_output_rows").alias("metric_output_rows"),
            F.col("details:flow_progress:status").alias("flow_status"),
            F.col("details:flow_progress:data_quality").alias("data_quality"),
        )
        .orderBy(F.col("timestamp").desc())
    )

    display(flow_progress_df)

# COMMAND ----------

if PIPELINE_ID:
    raw_events_df = spark.sql(f"SELECT * FROM event_log('{PIPELINE_ID}')")

    # Build flow metrics with LAG window
    flow_window = Window.partitionBy("flow_name").orderBy("timestamp")

    flow_metrics_df = (
        raw_events_df
        .filter(F.col("event_type") == "flow_progress")
        .filter(F.col("timestamp") >= F.current_timestamp() - F.expr(f"INTERVAL {LOOKBACK_DAYS} DAYS"))
        .select(
            F.col("origin.flow_name").alias("flow_name"),
            F.col("origin.dataset_name").alias("dataset_name"),
            F.col("timestamp"),
            F.col("details:flow_progress:num_output_rows").cast("bigint").alias("output_rows"),
        )
        .withColumn("prev_timestamp", F.lag("timestamp").over(flow_window))
    )

    # Calculate batch durations
    with_duration_df = (
        flow_metrics_df
        .filter(F.col("prev_timestamp").isNotNull())
        .withColumn(
            "batch_duration_seconds",
            F.unix_timestamp("timestamp") - F.unix_timestamp("prev_timestamp"),
        )
        .filter(F.col("batch_duration_seconds") > 0)
    )

    # Aggregate throughput stats
    throughput_df = (
        with_duration_df
        .groupBy("flow_name", "dataset_name")
        .agg(
            F.count("*").alias("total_batches"),
            F.round(F.avg("batch_duration_seconds"), 2).alias("avg_batch_duration_sec"),
            F.round(F.min("batch_duration_seconds"), 2).alias("min_batch_duration_sec"),
            F.round(F.max("batch_duration_seconds"), 2).alias("max_batch_duration_sec"),
            F.round(F.percentile_approx("batch_duration_seconds", 0.95), 2).alias("p95_batch_duration_sec"),
            F.sum("output_rows").alias("total_rows_processed"),
            F.round(
                F.sum("output_rows") / F.when(F.sum("batch_duration_seconds") != 0, F.sum("batch_duration_seconds")),
                2,
            ).alias("avg_rows_per_second"),
        )
        .orderBy(F.col("avg_rows_per_second").desc())
    )

    display(throughput_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Backlog Monitoring
# MAGIC
# MAGIC Track `backlog_bytes` and `backlog_files` to proactively tune Kafka/Kinesis offsets
# MAGIC or Auto Loader instances before downstream systems are impacted.

# COMMAND ----------

if PIPELINE_ID:
    raw_events_df = spark.sql(f"SELECT * FROM event_log('{PIPELINE_ID}')")

    backlog_df = (
        raw_events_df
        .filter(F.col("event_type") == "flow_progress")
        .filter(
            F.col("details:flow_progress:metrics:backlog_bytes").isNotNull()
            | F.col("details:flow_progress:metrics:backlog_files").isNotNull()
        )
        .filter(F.col("timestamp") >= F.current_timestamp() - F.expr(f"INTERVAL {LOOKBACK_DAYS} DAYS"))
        .select(
            F.col("timestamp"),
            F.col("origin.flow_name").alias("flow_name"),
            F.col("origin.dataset_name").alias("dataset_name"),
            F.col("details:flow_progress:metrics:backlog_bytes").cast("bigint").alias("backlog_bytes"),
            F.col("details:flow_progress:metrics:backlog_files").cast("bigint").alias("backlog_files"),
            F.col("details:flow_progress:metrics:num_output_rows").cast("bigint").alias("output_rows"),
        )
        .orderBy(F.col("timestamp").desc())
    )

    display(backlog_df)

# COMMAND ----------

if PIPELINE_ID:
    raw_events_df = spark.sql(f"SELECT * FROM event_log('{PIPELINE_ID}')")

    # Daily backlog aggregation
    backlog_daily_df = (
        raw_events_df
        .filter(F.col("event_type") == "flow_progress")
        .filter(F.col("details:flow_progress:metrics:backlog_bytes").isNotNull())
        .filter(F.col("timestamp") >= F.current_timestamp() - F.expr(f"INTERVAL {LOOKBACK_DAYS} DAYS"))
        .withColumn("flow_name", F.col("origin.flow_name"))
        .withColumn("log_date", F.to_date("timestamp"))
        .withColumn("backlog_bytes_val", F.col("details:flow_progress:metrics:backlog_bytes").cast("bigint"))
        .withColumn("backlog_files_val", F.col("details:flow_progress:metrics:backlog_files").cast("bigint"))
        .groupBy("flow_name", "log_date")
        .agg(
            F.avg("backlog_bytes_val").alias("avg_backlog_bytes"),
            F.max("backlog_bytes_val").alias("max_backlog_bytes"),
            F.avg("backlog_files_val").alias("avg_backlog_files"),
            F.max("backlog_files_val").alias("max_backlog_files"),
        )
    )

    # Backlog trend with LAG for day-over-day growth
    trend_window = Window.partitionBy("flow_name").orderBy("log_date")

    backlog_trend_df = (
        backlog_daily_df
        .withColumn("prev_day_avg_bytes", F.lag("avg_backlog_bytes").over(trend_window))
        .withColumn(
            "backlog_growth_pct",
            F.when(
                F.col("prev_day_avg_bytes") > 0,
                F.round(
                    (F.col("avg_backlog_bytes") - F.col("prev_day_avg_bytes"))
                    / F.col("prev_day_avg_bytes") * 100,
                    2,
                ),
            ),
        )
        .orderBy("flow_name", F.col("log_date").desc())
    )

    display(backlog_trend_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Resource Utilization
# MAGIC
# MAGIC Monitor average task slots and queued tasks to optimize compute scaling.

# COMMAND ----------

if PIPELINE_ID:
    raw_events_df = spark.sql(f"SELECT * FROM event_log('{PIPELINE_ID}')")

    resource_df = (
        raw_events_df
        .filter(F.col("event_type") == "flow_progress")
        .filter(
            F.col("details:flow_progress:metrics:num_task_slots").isNotNull()
            | F.col("details:flow_progress:metrics:num_queued_tasks").isNotNull()
        )
        .filter(F.col("timestamp") >= F.current_timestamp() - F.expr(f"INTERVAL {LOOKBACK_DAYS} DAYS"))
        .select(
            F.col("timestamp"),
            F.col("origin.flow_name").alias("flow_name"),
            F.col("details:flow_progress:metrics:num_task_slots").cast("int").alias("num_task_slots"),
            F.col("details:flow_progress:metrics:num_queued_tasks").cast("int").alias("num_queued_tasks"),
            F.col("details:flow_progress:metrics:num_active_tasks").cast("int").alias("num_active_tasks"),
        )
        .orderBy(F.col("timestamp").desc())
    )

    display(resource_df)

# COMMAND ----------

if PIPELINE_ID:
    raw_events_df = spark.sql(f"SELECT * FROM event_log('{PIPELINE_ID}')")

    # Resource utilization summary
    resource_summary_df = (
        raw_events_df
        .filter(F.col("event_type") == "flow_progress")
        .filter(F.col("details:flow_progress:metrics:num_task_slots").isNotNull())
        .filter(F.col("timestamp") >= F.current_timestamp() - F.expr(f"INTERVAL {LOOKBACK_DAYS} DAYS"))
        .withColumn("flow_name", F.col("origin.flow_name"))
        .withColumn("task_slots", F.col("details:flow_progress:metrics:num_task_slots").cast("int"))
        .withColumn("queued_tasks", F.col("details:flow_progress:metrics:num_queued_tasks").cast("int"))
        .withColumn("active_tasks", F.col("details:flow_progress:metrics:num_active_tasks").cast("int"))
        .groupBy("flow_name")
        .agg(
            F.round(F.avg("task_slots"), 2).alias("avg_task_slots"),
            F.round(F.avg("queued_tasks"), 2).alias("avg_queued_tasks"),
            F.round(F.avg("active_tasks"), 2).alias("avg_active_tasks"),
            F.max("queued_tasks").alias("max_queued_tasks"),
            F.round(
                F.avg("active_tasks")
                / F.when(F.avg("task_slots") != 0, F.avg("task_slots"))
                * 100,
                2,
            ).alias("avg_utilization_pct"),
        )
        .orderBy(F.col("avg_utilization_pct").desc())
    )

    display(resource_summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. StreamingQueryListener Metrics (Optional)
# MAGIC
# MAGIC If a custom `StreamingQueryListener` is configured to write metrics to a Delta table,
# MAGIC query it for additional streaming insights.

# COMMAND ----------

if STREAMING_METRICS_TABLE:
    listener_df = (
        spark.table(STREAMING_METRICS_TABLE)
        .filter(F.col("timestamp") >= F.current_timestamp() - F.expr(f"INTERVAL {LOOKBACK_DAYS} DAYS"))
        .select(
            "query_name",
            "batch_id",
            "timestamp",
            "num_input_rows",
            "input_rows_per_second",
            "processed_rows_per_second",
            "batch_duration_ms",
            F.round(F.col("batch_duration_ms") / 1000.0, 2).alias("batch_duration_sec"),
        )
        .orderBy(F.col("timestamp").desc())
    )

    display(listener_df)

    # Summary statistics per query
    listener_summary_df = (
        spark.table(STREAMING_METRICS_TABLE)
        .filter(F.col("timestamp") >= F.current_timestamp() - F.expr(f"INTERVAL {LOOKBACK_DAYS} DAYS"))
        .groupBy("query_name")
        .agg(
            F.count("*").alias("total_batches"),
            F.round(F.avg("batch_duration_ms") / 1000.0, 2).alias("avg_batch_duration_sec"),
            F.round(F.avg("input_rows_per_second"), 2).alias("avg_input_rows_per_sec"),
            F.round(F.avg("processed_rows_per_second"), 2).alias("avg_processed_rows_per_sec"),
            F.sum("num_input_rows").alias("total_input_rows"),
            F.round(F.percentile_approx("batch_duration_ms", 0.95) / 1000.0, 2).alias("p95_batch_duration_sec"),
        )
        .orderBy(F.col("avg_batch_duration_sec").desc())
    )

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
    raw_events_df = spark.sql(f"SELECT * FROM event_log('{PIPELINE_ID}')")

    # Recent backlogs (last 1 day)
    recent_backlogs_df = (
        raw_events_df
        .filter(F.col("event_type") == "flow_progress")
        .filter(F.col("details:flow_progress:metrics:backlog_bytes").isNotNull())
        .filter(F.col("timestamp") >= F.current_timestamp() - F.expr("INTERVAL 1 DAY"))
        .withColumn("flow_name", F.col("origin.flow_name"))
        .withColumn("backlog_bytes_val", F.col("details:flow_progress:metrics:backlog_bytes").cast("bigint"))
        .groupBy("flow_name")
        .agg(
            F.avg("backlog_bytes_val").alias("avg_backlog_bytes"),
            F.max("backlog_bytes_val").alias("max_backlog_bytes"),
        )
    )

    # Recent utilization (last 1 day)
    recent_utilization_df = (
        raw_events_df
        .filter(F.col("event_type") == "flow_progress")
        .filter(F.col("details:flow_progress:metrics:num_task_slots").isNotNull())
        .filter(F.col("timestamp") >= F.current_timestamp() - F.expr("INTERVAL 1 DAY"))
        .withColumn("flow_name", F.col("origin.flow_name"))
        .withColumn("task_slots", F.col("details:flow_progress:metrics:num_task_slots").cast("int"))
        .withColumn("queued_tasks", F.col("details:flow_progress:metrics:num_queued_tasks").cast("int"))
        .withColumn("active_tasks", F.col("details:flow_progress:metrics:num_active_tasks").cast("int"))
        .groupBy("flow_name")
        .agg(
            F.avg("queued_tasks").alias("avg_queued"),
            (F.avg("active_tasks") / F.when(F.avg("task_slots") != 0, F.avg("task_slots")) * 100).alias("utilization_pct"),
        )
    )

    # Full outer join and build alert columns
    streaming_alerts_df = (
        recent_backlogs_df.alias("b")
        .join(recent_utilization_df.alias("u"), on="flow_name", how="full_outer")
        .select(
            F.col("flow_name"),
            F.col("b.avg_backlog_bytes"),
            F.col("b.max_backlog_bytes"),
            F.round(F.col("u.utilization_pct"), 2).alias("utilization_pct"),
            F.round(F.col("u.avg_queued"), 2).alias("avg_queued_tasks"),
            F.when(F.col("b.max_backlog_bytes") > 1073741824, F.lit("CRITICAL"))
            .when(F.col("b.max_backlog_bytes") > 104857600, F.lit("WARNING"))
            .otherwise(F.lit("OK"))
            .alias("backlog_alert"),
            F.when(F.col("u.utilization_pct") > 90, F.lit("WARNING"))
            .otherwise(F.lit("OK"))
            .alias("utilization_alert"),
        )
    )

    streaming_alerts_df.createOrReplaceTempView("streaming_alerts")
    display(spark.table("streaming_alerts"))
