# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 3: Data Lineage and Impact Analysis
# MAGIC
# MAGIC Trace the upstream origins of datasets and understand the downstream impact of potential
# MAGIC data issues or schema changes using Unity Catalog lineage system tables.
# MAGIC
# MAGIC **Data Sources:**
# MAGIC - `system.access.table_lineage`
# MAGIC - `system.access.column_lineage`
# MAGIC - Fallback: Data Lineage REST API `/api/2.0/lineage-tracking/table-lineage`
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Unity Catalog enabled
# MAGIC - `system.access` schema enabled by an account admin
# MAGIC - User has at least `BROWSE` permissions on target tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("target_table", "", "Target Table (catalog.schema.table)")
dbutils.widgets.text("target_column", "", "Target Column (optional)")
dbutils.widgets.dropdown("use_rest_api", "false", ["true", "false"], "Use REST API Fallback")

TARGET_TABLE = dbutils.widgets.get("target_table")
TARGET_COLUMN = dbutils.widgets.get("target_column")
USE_REST_API = dbutils.widgets.get("use_rest_api") == "true"

LOOKBACK_DAYS = 30

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from table_config import TABLE_LINEAGE, COLUMN_LINEAGE

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Upstream Tracking (Parent Tables)
# MAGIC
# MAGIC Identify all source tables that feed into the target table.

# COMMAND ----------

# All upstream (source) tables for a given target
table_lineage_df = spark.table(TABLE_LINEAGE)

upstream_direct_df = (
    table_lineage_df
    .filter(F.col("target_table_full_name") == TARGET_TABLE)
    .filter(F.col("event_time") >= F.expr(f"CURRENT_TIMESTAMP() - INTERVAL {LOOKBACK_DAYS} DAYS"))
    .select(
        F.col("source_table_full_name").alias("upstream_table"),
        F.col("source_type"),
        F.col("target_table_full_name").alias("downstream_table"),
        F.col("target_type"),
        F.col("created_by"),
        F.col("entity_type").alias("transformation_entity"),
        F.col("entity_run_id"),
        F.col("event_time"),
    )
    .orderBy(F.col("event_time").desc())
)

display(upstream_direct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Downstream Tracking (Child Tables)
# MAGIC
# MAGIC Identify all tables that depend on the target table. Useful for impact analysis
# MAGIC before schema changes or data fixes.

# COMMAND ----------

# All downstream (dependent) tables from a given source
downstream_direct_df = (
    table_lineage_df
    .filter(F.col("source_table_full_name") == TARGET_TABLE)
    .filter(F.col("event_time") >= F.expr(f"CURRENT_TIMESTAMP() - INTERVAL {LOOKBACK_DAYS} DAYS"))
    .select(
        F.col("target_table_full_name").alias("downstream_table"),
        F.col("target_type"),
        F.col("source_table_full_name").alias("upstream_table"),
        F.col("source_type"),
        F.col("created_by"),
        F.col("entity_type").alias("transformation_entity"),
        F.col("entity_run_id"),
        F.col("event_time"),
    )
    .orderBy(F.col("event_time").desc())
)

display(downstream_direct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Full Lineage Graph
# MAGIC
# MAGIC Multi-hop lineage: trace the complete dependency chain for the target table.

# COMMAND ----------

# Build a multi-hop lineage graph using iterative traversal
if TARGET_TABLE:
    lineage_df = spark.table(TABLE_LINEAGE)
    lineage_df = lineage_df.filter(
        F.col("event_time") >= F.expr(f"CURRENT_TIMESTAMP() - INTERVAL {LOOKBACK_DAYS} DAYS")
    )

    # --- Upstream traversal (parents of parents, up to depth 5) ---
    current_targets = [TARGET_TABLE]
    all_upstream = []

    for depth in range(1, 6):
        hop = (
            lineage_df
            .filter(F.col("target_table_full_name").isin(current_targets))
            .select(
                F.col("source_table_full_name").alias("upstream_table"),
                F.col("target_table_full_name").alias("feeds_into"),
                F.col("entity_type").alias("transformation"),
                F.col("created_by"),
            )
            .distinct()
            .withColumn("depth", F.lit(depth))
        )

        if hop.count() == 0:
            break

        all_upstream.append(hop)
        current_targets = [
            row.upstream_table
            for row in hop.select("upstream_table").distinct().collect()
        ]

    if all_upstream:
        from functools import reduce
        upstream_df = reduce(lambda a, b: a.unionByName(b), all_upstream)
        upstream_df = upstream_df.orderBy("depth", "upstream_table")
        display(upstream_df)
    else:
        print("No upstream lineage found for the target table.")

    # --- Downstream traversal (children of children, up to depth 5) ---
    current_sources = [TARGET_TABLE]
    all_downstream = []

    for depth in range(1, 6):
        hop = (
            lineage_df
            .filter(F.col("source_table_full_name").isin(current_sources))
            .select(
                F.col("target_table_full_name").alias("downstream_table"),
                F.col("source_table_full_name").alias("fed_by"),
                F.col("entity_type").alias("transformation"),
                F.col("created_by"),
            )
            .distinct()
            .withColumn("depth", F.lit(depth))
        )

        if hop.count() == 0:
            break

        all_downstream.append(hop)
        current_sources = [
            row.downstream_table
            for row in hop.select("downstream_table").distinct().collect()
        ]

    if all_downstream:
        from functools import reduce
        downstream_df = reduce(lambda a, b: a.unionByName(b), all_downstream)
        downstream_df = downstream_df.orderBy("depth", "downstream_table")
        display(downstream_df)
    else:
        print("No downstream lineage found for the target table.")
else:
    print("WARNING: No target_table provided. Set the widget value to trace lineage.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Column-Level Lineage
# MAGIC
# MAGIC Identify how specific fields are derived to prevent compliance violations
# MAGIC or data leakage.

# COMMAND ----------

# Column-level upstream lineage
column_lineage_df = spark.table(COLUMN_LINEAGE)

col_upstream_df = (
    column_lineage_df
    .filter(F.col("target_table_full_name") == TARGET_TABLE)
    .filter(F.col("event_time") >= F.expr(f"CURRENT_TIMESTAMP() - INTERVAL {LOOKBACK_DAYS} DAYS"))
)

# Apply optional column filter
if TARGET_COLUMN:
    col_upstream_df = col_upstream_df.filter(F.col("target_column_name") == TARGET_COLUMN)

col_upstream_df = (
    col_upstream_df
    .select(
        F.col("source_table_full_name").alias("source_table"),
        F.col("source_column_name").alias("source_column"),
        F.col("target_table_full_name").alias("target_table"),
        F.col("target_column_name").alias("target_column"),
        F.col("event_time"),
    )
    .orderBy("target_column", F.col("event_time").desc())
)

display(col_upstream_df)

# COMMAND ----------

# Column-level downstream impact: where does a source column flow?
col_downstream_df = (
    column_lineage_df
    .filter(F.col("source_table_full_name") == TARGET_TABLE)
    .filter(F.col("event_time") >= F.expr(f"CURRENT_TIMESTAMP() - INTERVAL {LOOKBACK_DAYS} DAYS"))
)

# Apply optional column filter
if TARGET_COLUMN:
    col_downstream_df = col_downstream_df.filter(F.col("source_column_name") == TARGET_COLUMN)

col_downstream_df = (
    col_downstream_df
    .select(
        F.col("target_table_full_name").alias("target_table"),
        F.col("target_column_name").alias("target_column"),
        F.col("source_table_full_name").alias("source_table"),
        F.col("source_column_name").alias("source_column"),
        F.col("event_time"),
    )
    .orderBy("target_table", "target_column", F.col("event_time").desc())
)

display(col_downstream_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. REST API Fallback
# MAGIC
# MAGIC For regions that don't support lineage system tables, use the Data Lineage REST API.

# COMMAND ----------

import json
import requests

if USE_REST_API and TARGET_TABLE:
    # Parse table identifier
    parts = TARGET_TABLE.split(".")
    if len(parts) != 3:
        raise ValueError("target_table must be in format: catalog.schema.table")

    catalog, schema, table = parts

    # Get workspace URL and token from the current context
    workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

    headers = {"Authorization": f"Bearer {token}"}

    # Fetch table lineage via REST API
    api_url = f"https://{workspace_url}/api/2.0/lineage-tracking/table-lineage"
    payload = {
        "table_name": TARGET_TABLE
    }

    response = requests.get(api_url, headers=headers, json=payload)

    if response.status_code == 200:
        lineage_data = response.json()

        # Display upstream
        upstreams = lineage_data.get("upstreams", [])
        if upstreams:
            upstream_rows = [
                {
                    "source_table": u.get("tableInfo", {}).get("name", "N/A"),
                    "source_type": u.get("tableInfo", {}).get("table_type", "N/A"),
                    "notebook_id": u.get("notebookInfos", [{}])[0].get("notebook_id", "N/A") if u.get("notebookInfos") else "N/A"
                }
                for u in upstreams
            ]
            display(spark.createDataFrame(upstream_rows))

        # Display downstream
        downstreams = lineage_data.get("downstreams", [])
        if downstreams:
            downstream_rows = [
                {
                    "target_table": d.get("tableInfo", {}).get("name", "N/A"),
                    "target_type": d.get("tableInfo", {}).get("table_type", "N/A"),
                    "notebook_id": d.get("notebookInfos", [{}])[0].get("notebook_id", "N/A") if d.get("notebookInfos") else "N/A"
                }
                for d in downstreams
            ]
            display(spark.createDataFrame(downstream_rows))
    else:
        print(f"REST API call failed with status {response.status_code}: {response.text}")
elif USE_REST_API:
    print("WARNING: REST API fallback enabled but no target_table provided.")
