# Databricks notebook source
# COMMAND ----------

# Centralized table name configuration for observability notebooks.
# Override these values to point at different catalogs/schemas.

# --- Billing & Cost (Notebook 01) ---
BILLING_USAGE = "system.billing.usage"
BILLING_LIST_PRICES = "system.billing.list_prices"

# --- Lakeflow / Jobs (Notebook 01) ---
LAKEFLOW_JOBS = "system.lakeflow.jobs"
LAKEFLOW_JOB_RUN_TIMELINE = "system.lakeflow.job_run_timeline"

# --- Lineage (Notebook 03) ---
TABLE_LINEAGE = "system.access.table_lineage"
COLUMN_LINEAGE = "system.access.column_lineage"
