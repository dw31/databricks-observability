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

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW daily_spend AS
# MAGIC SELECT
# MAGIC   u.usage_date,
# MAGIC   u.sku_name,
# MAGIC   u.usage_unit,
# MAGIC   SUM(u.usage_quantity) AS total_usage,
# MAGIC   SUM(u.usage_quantity * lp.pricing.default) AS total_cost
# MAGIC FROM system.billing.usage u
# MAGIC LEFT JOIN system.billing.list_prices lp
# MAGIC   ON u.sku_name = lp.sku_name
# MAGIC   AND u.usage_unit = lp.usage_unit
# MAGIC   AND u.usage_date BETWEEN lp.price_start_time AND COALESCE(lp.price_end_time, CURRENT_DATE())
# MAGIC WHERE u.usage_date >= CURRENT_DATE() - INTERVAL 60 DAYS
# MAGIC GROUP BY u.usage_date, u.sku_name, u.usage_unit

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 7-day and 14-day rolling spend with growth rates
# MAGIC WITH daily_total AS (
# MAGIC   SELECT
# MAGIC     usage_date,
# MAGIC     SUM(total_cost) AS day_cost
# MAGIC   FROM daily_spend
# MAGIC   GROUP BY usage_date
# MAGIC ),
# MAGIC rolling AS (
# MAGIC   SELECT
# MAGIC     usage_date,
# MAGIC     day_cost,
# MAGIC     SUM(day_cost) OVER (
# MAGIC       ORDER BY usage_date
# MAGIC       ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
# MAGIC     ) AS rolling_7d_cost,
# MAGIC     SUM(day_cost) OVER (
# MAGIC       ORDER BY usage_date
# MAGIC       ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
# MAGIC     ) AS rolling_14d_cost
# MAGIC   FROM daily_total
# MAGIC ),
# MAGIC with_prior AS (
# MAGIC   SELECT
# MAGIC     usage_date,
# MAGIC     day_cost,
# MAGIC     rolling_7d_cost,
# MAGIC     rolling_14d_cost,
# MAGIC     LAG(rolling_7d_cost, 7) OVER (ORDER BY usage_date) AS prior_7d_cost,
# MAGIC     LAG(rolling_14d_cost, 14) OVER (ORDER BY usage_date) AS prior_14d_cost
# MAGIC   FROM rolling
# MAGIC )
# MAGIC SELECT
# MAGIC   usage_date,
# MAGIC   day_cost,
# MAGIC   rolling_7d_cost,
# MAGIC   rolling_14d_cost,
# MAGIC   ROUND(
# MAGIC     CASE WHEN prior_7d_cost > 0
# MAGIC       THEN ((rolling_7d_cost - prior_7d_cost) / prior_7d_cost) * 100
# MAGIC       ELSE NULL
# MAGIC     END, 2
# MAGIC   ) AS spend_growth_7d_pct,
# MAGIC   ROUND(
# MAGIC     CASE WHEN prior_14d_cost > 0
# MAGIC       THEN ((rolling_14d_cost - prior_14d_cost) / prior_14d_cost) * 100
# MAGIC       ELSE NULL
# MAGIC     END, 2
# MAGIC   ) AS spend_growth_14d_pct
# MAGIC FROM with_prior
# MAGIC WHERE usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
# MAGIC ORDER BY usage_date DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Most Expensive Jobs
# MAGIC
# MAGIC Aggregate the total list cost per job run over the last 30 days.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   j.job_id,
# MAGIC   j.name AS job_name,
# MAGIC   j.creator,
# MAGIC   COUNT(DISTINCT rt.run_id) AS total_runs,
# MAGIC   ROUND(SUM(rt.result_execution_duration / 3600.0 * lp.pricing.default), 2) AS estimated_total_cost,
# MAGIC   ROUND(AVG(rt.result_execution_duration / 3600.0 * lp.pricing.default), 2) AS avg_cost_per_run,
# MAGIC   ROUND(SUM(rt.result_execution_duration) / 3600.0, 2) AS total_dbu_hours
# MAGIC FROM system.lakeflow.job_run_timeline rt
# MAGIC INNER JOIN system.lakeflow.jobs j
# MAGIC   ON rt.job_id = j.job_id
# MAGIC LEFT JOIN system.billing.list_prices lp
# MAGIC   ON lp.sku_name = 'JOBS_COMPUTE'
# MAGIC   AND rt.period_start_time BETWEEN lp.price_start_time AND COALESCE(lp.price_end_time, CURRENT_TIMESTAMP())
# MAGIC WHERE rt.period_start_time >= CURRENT_DATE() - INTERVAL 30 DAYS
# MAGIC GROUP BY j.job_id, j.name, j.creator
# MAGIC ORDER BY estimated_total_cost DESC
# MAGIC LIMIT 25

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Failed Jobs and Retry Patterns
# MAGIC
# MAGIC Calculate success ratios, failure costs, and frequent repair runs.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Job success/failure breakdown with cost impact
# MAGIC WITH run_results AS (
# MAGIC   SELECT
# MAGIC     j.job_id,
# MAGIC     j.name AS job_name,
# MAGIC     rt.run_id,
# MAGIC     rt.result_state,
# MAGIC     rt.result_execution_duration,
# MAGIC     CASE
# MAGIC       WHEN rt.result_state IN ('ERROR', 'FAILED', 'TIMED_OUT') THEN 'FAILURE'
# MAGIC       WHEN rt.result_state = 'SUCCESS' THEN 'SUCCESS'
# MAGIC       ELSE 'OTHER'
# MAGIC     END AS outcome
# MAGIC   FROM system.lakeflow.job_run_timeline rt
# MAGIC   INNER JOIN system.lakeflow.jobs j
# MAGIC     ON rt.job_id = j.job_id
# MAGIC   WHERE rt.period_start_time >= CURRENT_DATE() - INTERVAL 30 DAYS
# MAGIC )
# MAGIC SELECT
# MAGIC   job_id,
# MAGIC   job_name,
# MAGIC   COUNT(*) AS total_runs,
# MAGIC   SUM(CASE WHEN outcome = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
# MAGIC   SUM(CASE WHEN outcome = 'FAILURE' THEN 1 ELSE 0 END) AS failure_count,
# MAGIC   ROUND(
# MAGIC     SUM(CASE WHEN outcome = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
# MAGIC   ) AS success_rate_pct,
# MAGIC   ROUND(
# MAGIC     SUM(CASE WHEN outcome = 'FAILURE' THEN result_execution_duration ELSE 0 END) / 3600.0, 2
# MAGIC   ) AS wasted_compute_hours,
# MAGIC   SUM(CASE WHEN outcome = 'FAILURE' THEN 1 ELSE 0 END) AS repair_attempts
# MAGIC FROM run_results
# MAGIC GROUP BY job_id, job_name
# MAGIC HAVING failure_count > 0
# MAGIC ORDER BY failure_count DESC, wasted_compute_hours DESC
# MAGIC LIMIT 25

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Failure timeline: identify patterns in when failures occur
# MAGIC SELECT
# MAGIC   DATE(rt.period_start_time) AS failure_date,
# MAGIC   HOUR(rt.period_start_time) AS failure_hour,
# MAGIC   rt.result_state,
# MAGIC   COUNT(*) AS failure_count,
# MAGIC   COLLECT_SET(j.name) AS affected_jobs
# MAGIC FROM system.lakeflow.job_run_timeline rt
# MAGIC INNER JOIN system.lakeflow.jobs j
# MAGIC   ON rt.job_id = j.job_id
# MAGIC WHERE rt.result_state IN ('ERROR', 'FAILED', 'TIMED_OUT')
# MAGIC   AND rt.period_start_time >= CURRENT_DATE() - INTERVAL 30 DAYS
# MAGIC GROUP BY failure_date, failure_hour, rt.result_state
# MAGIC ORDER BY failure_date DESC, failure_hour DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Alerting Integration
# MAGIC
# MAGIC Summary view for integration with Databricks SQL Alerts.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Alert-ready view: cost spikes and high failure rates
# MAGIC CREATE OR REPLACE TEMP VIEW cost_alerts AS
# MAGIC WITH recent_spend AS (
# MAGIC   SELECT
# MAGIC     SUM(CASE WHEN usage_date >= CURRENT_DATE() - INTERVAL 7 DAYS THEN total_cost ELSE 0 END) AS last_7d_cost,
# MAGIC     SUM(CASE WHEN usage_date BETWEEN CURRENT_DATE() - INTERVAL 14 DAYS AND CURRENT_DATE() - INTERVAL 8 DAYS THEN total_cost ELSE 0 END) AS prev_7d_cost
# MAGIC   FROM daily_spend
# MAGIC ),
# MAGIC failure_summary AS (
# MAGIC   SELECT
# MAGIC     COUNT(*) AS total_failures,
# MAGIC     SUM(result_execution_duration) / 3600.0 AS total_wasted_hours
# MAGIC   FROM system.lakeflow.job_run_timeline
# MAGIC   WHERE result_state IN ('ERROR', 'FAILED', 'TIMED_OUT')
# MAGIC     AND period_start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
# MAGIC )
# MAGIC SELECT
# MAGIC   rs.last_7d_cost,
# MAGIC   rs.prev_7d_cost,
# MAGIC   ROUND(((rs.last_7d_cost - rs.prev_7d_cost) / NULLIF(rs.prev_7d_cost, 0)) * 100, 2) AS spend_growth_pct,
# MAGIC   fs.total_failures,
# MAGIC   ROUND(fs.total_wasted_hours, 2) AS wasted_compute_hours,
# MAGIC   CASE
# MAGIC     WHEN ((rs.last_7d_cost - rs.prev_7d_cost) / NULLIF(rs.prev_7d_cost, 0)) * 100 > ${SPEND_GROWTH_ALERT_PCT} THEN 'ALERT'
# MAGIC     ELSE 'OK'
# MAGIC   END AS cost_alert_status
# MAGIC FROM recent_spend rs
# MAGIC CROSS JOIN failure_summary fs;
# MAGIC
# MAGIC SELECT * FROM cost_alerts
