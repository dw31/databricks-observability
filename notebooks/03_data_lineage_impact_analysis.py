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

# MAGIC %md
# MAGIC ## 1. Upstream Tracking (Parent Tables)
# MAGIC
# MAGIC Identify all source tables that feed into the target table.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- All upstream (source) tables for a given target
# MAGIC SELECT
# MAGIC   source_table_full_name AS upstream_table,
# MAGIC   source_type,
# MAGIC   target_table_full_name AS downstream_table,
# MAGIC   target_type,
# MAGIC   created_by,
# MAGIC   entity_type AS transformation_entity,
# MAGIC   entity_run_id,
# MAGIC   event_time
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE target_table_full_name = '${target_table}'
# MAGIC   AND event_time >= CURRENT_TIMESTAMP() - INTERVAL ${LOOKBACK_DAYS} DAYS
# MAGIC ORDER BY event_time DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Downstream Tracking (Child Tables)
# MAGIC
# MAGIC Identify all tables that depend on the target table. Useful for impact analysis
# MAGIC before schema changes or data fixes.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- All downstream (dependent) tables from a given source
# MAGIC SELECT
# MAGIC   target_table_full_name AS downstream_table,
# MAGIC   target_type,
# MAGIC   source_table_full_name AS upstream_table,
# MAGIC   source_type,
# MAGIC   created_by,
# MAGIC   entity_type AS transformation_entity,
# MAGIC   entity_run_id,
# MAGIC   event_time
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE source_table_full_name = '${target_table}'
# MAGIC   AND event_time >= CURRENT_TIMESTAMP() - INTERVAL ${LOOKBACK_DAYS} DAYS
# MAGIC ORDER BY event_time DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Full Lineage Graph
# MAGIC
# MAGIC Multi-hop lineage: trace the complete dependency chain for the target table.

# COMMAND ----------

# Build a multi-hop lineage graph using iterative queries
from pyspark.sql import functions as F

if TARGET_TABLE:
    # Upstream traversal (recursive-like via iterative joins)
    upstream_df = spark.sql(f"""
        WITH RECURSIVE upstream_lineage AS (
            -- Base case: direct parents
            SELECT
                source_table_full_name AS table_name,
                target_table_full_name AS child_table,
                1 AS depth,
                entity_type AS transformation,
                created_by
            FROM system.access.table_lineage
            WHERE target_table_full_name = '{TARGET_TABLE}'
              AND event_time >= CURRENT_TIMESTAMP() - INTERVAL {LOOKBACK_DAYS} DAYS

            UNION ALL

            -- Recursive case: parents of parents
            SELECT
                tl.source_table_full_name,
                tl.target_table_full_name,
                ul.depth + 1,
                tl.entity_type,
                tl.created_by
            FROM system.access.table_lineage tl
            INNER JOIN upstream_lineage ul
              ON tl.target_table_full_name = ul.table_name
            WHERE ul.depth < 5
              AND tl.event_time >= CURRENT_TIMESTAMP() - INTERVAL {LOOKBACK_DAYS} DAYS
        )
        SELECT DISTINCT
            table_name AS upstream_table,
            child_table AS feeds_into,
            depth,
            transformation,
            created_by
        FROM upstream_lineage
        ORDER BY depth, upstream_table
    """)

    display(upstream_df)

    # Downstream traversal
    downstream_df = spark.sql(f"""
        WITH RECURSIVE downstream_lineage AS (
            SELECT
                target_table_full_name AS table_name,
                source_table_full_name AS parent_table,
                1 AS depth,
                entity_type AS transformation,
                created_by
            FROM system.access.table_lineage
            WHERE source_table_full_name = '{TARGET_TABLE}'
              AND event_time >= CURRENT_TIMESTAMP() - INTERVAL {LOOKBACK_DAYS} DAYS

            UNION ALL

            SELECT
                tl.target_table_full_name,
                tl.source_table_full_name,
                dl.depth + 1,
                tl.entity_type,
                tl.created_by
            FROM system.access.table_lineage tl
            INNER JOIN downstream_lineage dl
              ON tl.source_table_full_name = dl.table_name
            WHERE dl.depth < 5
              AND tl.event_time >= CURRENT_TIMESTAMP() - INTERVAL {LOOKBACK_DAYS} DAYS
        )
        SELECT DISTINCT
            table_name AS downstream_table,
            parent_table AS fed_by,
            depth,
            transformation,
            created_by
        FROM downstream_lineage
        ORDER BY depth, downstream_table
    """)

    display(downstream_df)
else:
    print("WARNING: No target_table provided. Set the widget value to trace lineage.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Column-Level Lineage
# MAGIC
# MAGIC Identify how specific fields are derived to prevent compliance violations
# MAGIC or data leakage.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Column-level upstream lineage
# MAGIC SELECT
# MAGIC   source_table_full_name AS source_table,
# MAGIC   source_column_name AS source_column,
# MAGIC   target_table_full_name AS target_table,
# MAGIC   target_column_name AS target_column,
# MAGIC   event_time
# MAGIC FROM system.access.column_lineage
# MAGIC WHERE target_table_full_name = '${target_table}'
# MAGIC   AND (
# MAGIC     '${target_column}' = ''
# MAGIC     OR target_column_name = '${target_column}'
# MAGIC   )
# MAGIC   AND event_time >= CURRENT_TIMESTAMP() - INTERVAL ${LOOKBACK_DAYS} DAYS
# MAGIC ORDER BY target_column_name, event_time DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Column-level downstream impact: where does a source column flow?
# MAGIC SELECT
# MAGIC   target_table_full_name AS target_table,
# MAGIC   target_column_name AS target_column,
# MAGIC   source_table_full_name AS source_table,
# MAGIC   source_column_name AS source_column,
# MAGIC   event_time
# MAGIC FROM system.access.column_lineage
# MAGIC WHERE source_table_full_name = '${target_table}'
# MAGIC   AND (
# MAGIC     '${target_column}' = ''
# MAGIC     OR source_column_name = '${target_column}'
# MAGIC   )
# MAGIC   AND event_time >= CURRENT_TIMESTAMP() - INTERVAL ${LOOKBACK_DAYS} DAYS
# MAGIC ORDER BY target_table, target_column_name, event_time DESC

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
