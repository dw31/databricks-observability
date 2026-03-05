# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 1: Cost and Operational Health Observability
# MAGIC
# MAGIC Track job costs, identify the most expensive pipelines, and monitor retry/failure patterns
# MAGIC to optimize Databricks spend.
# MAGIC
# MAGIC **Data Sources:**
# MAGIC - `system.billing.usage`
# MAGIC - `system.billing.list_prices`
# MAGIC - `system.lakeflow.jobs`
# MAGIC - `system.lakeflow.job_run_timeline`
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Unity Catalog enabled
# MAGIC - `system.billing` and `system.lakeflow` schemas enabled by an account admin
# MAGIC - User has `USE` and `SELECT` on the required system schemas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from table_config import BILLING_USAGE, BILLING_LIST_PRICES, LAKEFLOW_JOBS, LAKEFLOW_JOB_RUN_TIMELINE

# COMMAND ----------

# Lookback windows (days)
SPEND_LOOKBACK_DAYS = 30
TREND_SHORT_WINDOW = 7
TREND_LONG_WINDOW = 14

# Alert thresholds
SPEND_GROWTH_ALERT_PCT = 25.0  # Alert if spend growth exceeds this percentage
FAILURE_COST_ALERT_THRESHOLD = 100.0  # Alert if wasted failure cost exceeds this (dollars)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Spend Trend Analysis
# MAGIC
# MAGIC Calculate 7-day and 14-day spend growth to identify sudden spikes in job costs.

# COMMAND ----------

# Build daily_spend DataFrame
usage_df = spark.table(BILLING_USAGE).alias("u")
list_prices_df = spark.table(BILLING_LIST_PRICES).alias("lp")

daily_spend_df = (
    usage_df
    .join(
        list_prices_df,
        (F.col("u.sku_name") == F.col("lp.sku_name"))
        & (F.col("u.usage_unit") == F.col("lp.usage_unit"))
        & (F.col("u.usage_date").between(F.col("lp.price_start_time"), F.coalesce(F.col("lp.price_end_time"), F.current_date()))),
        "left",
    )
    .where(F.col("u.usage_date") >= F.date_sub(F.current_date(), 60))
    .groupBy(F.col("u.usage_date").alias("usage_date"), F.col("u.sku_name").alias("sku_name"), F.col("u.usage_unit").alias("usage_unit"))
    .agg(
        F.sum("u.usage_quantity").alias("total_usage"),
        F.sum(F.col("u.usage_quantity") * F.col("lp.pricing.default")).alias("total_cost"),
    )
)

daily_spend_df.createOrReplaceTempView("daily_spend")
display(daily_spend_df)

# COMMAND ----------

# 7-day and 14-day rolling spend with growth rates
daily_total_df = (
    daily_spend_df
    .groupBy("usage_date")
    .agg(F.sum("total_cost").alias("day_cost"))
)

date_window = Window.orderBy("usage_date")
rolling_7d_window = Window.orderBy("usage_date").rowsBetween(-6, 0)
rolling_14d_window = Window.orderBy("usage_date").rowsBetween(-13, 0)

rolling_df = (
    daily_total_df
    .withColumn("rolling_7d_cost", F.sum("day_cost").over(rolling_7d_window))
    .withColumn("rolling_14d_cost", F.sum("day_cost").over(rolling_14d_window))
)

with_prior_df = (
    rolling_df
    .withColumn("prior_7d_cost", F.lag("rolling_7d_cost", 7).over(date_window))
    .withColumn("prior_14d_cost", F.lag("rolling_14d_cost", 14).over(date_window))
)

spend_trend_df = (
    with_prior_df
    .withColumn(
        "spend_growth_7d_pct",
        F.round(
            F.when(F.col("prior_7d_cost") > 0,
                   ((F.col("rolling_7d_cost") - F.col("prior_7d_cost")) / F.col("prior_7d_cost")) * 100)
            .otherwise(F.lit(None)),
            2,
        ),
    )
    .withColumn(
        "spend_growth_14d_pct",
        F.round(
            F.when(F.col("prior_14d_cost") > 0,
                   ((F.col("rolling_14d_cost") - F.col("prior_14d_cost")) / F.col("prior_14d_cost")) * 100)
            .otherwise(F.lit(None)),
            2,
        ),
    )
    .where(F.col("usage_date") >= F.date_sub(F.current_date(), SPEND_LOOKBACK_DAYS))
    .select("usage_date", "day_cost", "rolling_7d_cost", "rolling_14d_cost", "spend_growth_7d_pct", "spend_growth_14d_pct")
    .orderBy(F.col("usage_date").desc())
)

display(spend_trend_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Most Expensive Jobs
# MAGIC
# MAGIC Aggregate the total list cost per job run over the last 30 days.

# COMMAND ----------

run_timeline_df = spark.table(LAKEFLOW_JOB_RUN_TIMELINE).alias("rt")
jobs_df = spark.table(LAKEFLOW_JOBS).alias("j")
lp_df = spark.table(BILLING_LIST_PRICES).alias("lp")

expensive_jobs_df = (
    run_timeline_df
    .join(jobs_df, F.col("rt.job_id") == F.col("j.job_id"), "inner")
    .join(
        lp_df,
        (F.col("lp.sku_name") == F.lit("JOBS_COMPUTE"))
        & (F.col("rt.period_start_time").between(F.col("lp.price_start_time"), F.coalesce(F.col("lp.price_end_time"), F.current_timestamp()))),
        "left",
    )
    .where(F.col("rt.period_start_time") >= F.date_sub(F.current_date(), SPEND_LOOKBACK_DAYS))
    .groupBy(F.col("j.job_id").alias("job_id"), F.col("j.name").alias("job_name"), F.col("j.creator").alias("creator"))
    .agg(
        F.countDistinct("rt.run_id").alias("total_runs"),
        F.round(F.sum(F.col("rt.result_execution_duration") / 3600.0 * F.col("lp.pricing.default")), 2).alias("estimated_total_cost"),
        F.round(F.avg(F.col("rt.result_execution_duration") / 3600.0 * F.col("lp.pricing.default")), 2).alias("avg_cost_per_run"),
        F.round(F.sum(F.col("rt.result_execution_duration")) / 3600.0, 2).alias("total_dbu_hours"),
    )
    .orderBy(F.col("estimated_total_cost").desc())
    .limit(25)
)

display(expensive_jobs_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Failed Jobs and Retry Patterns
# MAGIC
# MAGIC Calculate success ratios, failure costs, and frequent repair runs.

# COMMAND ----------

# Job success/failure breakdown with cost impact
run_timeline_df2 = spark.table(LAKEFLOW_JOB_RUN_TIMELINE).alias("rt")
jobs_df2 = spark.table(LAKEFLOW_JOBS).alias("j")

run_results_df = (
    run_timeline_df2
    .join(jobs_df2, F.col("rt.job_id") == F.col("j.job_id"), "inner")
    .where(F.col("rt.period_start_time") >= F.date_sub(F.current_date(), SPEND_LOOKBACK_DAYS))
    .withColumn(
        "outcome",
        F.when(F.col("rt.result_state").isin("ERROR", "FAILED", "TIMED_OUT"), F.lit("FAILURE"))
        .when(F.col("rt.result_state") == "SUCCESS", F.lit("SUCCESS"))
        .otherwise(F.lit("OTHER")),
    )
    .select(
        F.col("j.job_id").alias("job_id"),
        F.col("j.name").alias("job_name"),
        F.col("rt.run_id"),
        F.col("rt.result_state"),
        F.col("rt.result_execution_duration"),
        "outcome",
    )
)

failure_breakdown_df = (
    run_results_df
    .groupBy("job_id", "job_name")
    .agg(
        F.count("*").alias("total_runs"),
        F.sum(F.when(F.col("outcome") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
        F.sum(F.when(F.col("outcome") == "FAILURE", 1).otherwise(0)).alias("failure_count"),
        F.round(
            F.sum(F.when(F.col("outcome") == "SUCCESS", 1).otherwise(0)) * 100.0 / F.count("*"), 2
        ).alias("success_rate_pct"),
        F.round(
            F.sum(F.when(F.col("outcome") == "FAILURE", F.col("result_execution_duration")).otherwise(0)) / 3600.0, 2
        ).alias("wasted_compute_hours"),
        F.sum(F.when(F.col("outcome") == "FAILURE", 1).otherwise(0)).alias("repair_attempts"),
    )
    .where(F.col("failure_count") > 0)
    .orderBy(F.col("failure_count").desc(), F.col("wasted_compute_hours").desc())
    .limit(25)
)

display(failure_breakdown_df)

# COMMAND ----------

# Failure timeline: identify patterns in when failures occur
run_timeline_df3 = spark.table(LAKEFLOW_JOB_RUN_TIMELINE).alias("rt")
jobs_df3 = spark.table(LAKEFLOW_JOBS).alias("j")

failure_timeline_df = (
    run_timeline_df3
    .join(jobs_df3, F.col("rt.job_id") == F.col("j.job_id"), "inner")
    .where(
        F.col("rt.result_state").isin("ERROR", "FAILED", "TIMED_OUT")
        & (F.col("rt.period_start_time") >= F.date_sub(F.current_date(), SPEND_LOOKBACK_DAYS))
    )
    .withColumn("failure_date", F.to_date("rt.period_start_time"))
    .withColumn("failure_hour", F.hour("rt.period_start_time"))
    .groupBy("failure_date", "failure_hour", F.col("rt.result_state").alias("result_state"))
    .agg(
        F.count("*").alias("failure_count"),
        F.collect_set("j.name").alias("affected_jobs"),
    )
    .orderBy(F.col("failure_date").desc(), F.col("failure_hour").desc())
)

display(failure_timeline_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Alerting Integration
# MAGIC
# MAGIC Summary view for integration with Databricks SQL Alerts.

# COMMAND ----------

# Alert-ready view: cost spikes and high failure rates

# Recent spend: last 7 days vs previous 7 days
daily_spend_cached = spark.table("daily_spend")

recent_spend_df = daily_spend_cached.select(
    F.sum(
        F.when(F.col("usage_date") >= F.date_sub(F.current_date(), 7), F.col("total_cost")).otherwise(0)
    ).alias("last_7d_cost"),
    F.sum(
        F.when(
            (F.col("usage_date") >= F.date_sub(F.current_date(), 14))
            & (F.col("usage_date") <= F.date_sub(F.current_date(), 8)),
            F.col("total_cost"),
        ).otherwise(0)
    ).alias("prev_7d_cost"),
)

# Failure summary: last 7 days
run_timeline_df4 = spark.table(LAKEFLOW_JOB_RUN_TIMELINE)

failure_summary_df = (
    run_timeline_df4
    .where(
        F.col("result_state").isin("ERROR", "FAILED", "TIMED_OUT")
        & (F.col("period_start_time") >= F.date_sub(F.current_date(), 7))
    )
    .select(
        F.count("*").alias("total_failures"),
        (F.sum("result_execution_duration") / 3600.0).alias("total_wasted_hours"),
    )
)

# Cross join and compute alert status
cost_alerts_df = (
    recent_spend_df.crossJoin(failure_summary_df)
    .select(
        F.col("last_7d_cost"),
        F.col("prev_7d_cost"),
        F.round(
            ((F.col("last_7d_cost") - F.col("prev_7d_cost")) / F.nullif(F.col("prev_7d_cost"), F.lit(0))) * 100, 2
        ).alias("spend_growth_pct"),
        F.col("total_failures"),
        F.round(F.col("total_wasted_hours"), 2).alias("wasted_compute_hours"),
        F.when(
            ((F.col("last_7d_cost") - F.col("prev_7d_cost")) / F.nullif(F.col("prev_7d_cost"), F.lit(0))) * 100 > SPEND_GROWTH_ALERT_PCT,
            F.lit("ALERT"),
        ).otherwise(F.lit("OK")).alias("cost_alert_status"),
    )
)

cost_alerts_df.createOrReplaceTempView("cost_alerts")
display(cost_alerts_df)
