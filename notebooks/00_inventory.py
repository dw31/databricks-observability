# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 0: Unity Catalog Inventory
# MAGIC
# MAGIC Enumerate all catalogs, schemas, tables, views, volumes, functions, and models
# MAGIC within a Unity Catalog metastore. Collect detailed statistics including row counts,
# MAGIC column listings, storage size, and object metadata.
# MAGIC
# MAGIC **Data Sources:**
# MAGIC - `system.information_schema` views
# MAGIC - `DESCRIBE DETAIL` / `DESCRIBE TABLE` commands
# MAGIC - Unity Catalog `system.access` metadata
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Unity Catalog enabled
# MAGIC - User has `BROWSE` or `USE`/`SELECT` on target catalogs/schemas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog_filter", "*", "Catalog Filter (* = all)")
dbutils.widgets.text("schema_filter", "*", "Schema Filter (* = all)")
dbutils.widgets.dropdown("collect_row_counts", "true", ["true", "false"], "Collect Row Counts (slower)")
dbutils.widgets.dropdown("collect_storage_stats", "true", ["true", "false"], "Collect Storage Stats (slower)")
dbutils.widgets.text("exclude_catalogs", "system,__databricks_internal", "Exclude Catalogs (comma-separated)")

CATALOG_FILTER = dbutils.widgets.get("catalog_filter")
SCHEMA_FILTER = dbutils.widgets.get("schema_filter")
COLLECT_ROW_COUNTS = dbutils.widgets.get("collect_row_counts") == "true"
COLLECT_STORAGE_STATS = dbutils.widgets.get("collect_storage_stats") == "true"
EXCLUDE_CATALOGS = [c.strip() for c in dbutils.widgets.get("exclude_catalogs").split(",") if c.strip()]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Catalog and Schema Enumeration

# COMMAND ----------

from pyspark.sql import functions as F, Row
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType, ArrayType
)
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback

# COMMAND ----------

# List all catalogs
all_catalogs_df = spark.sql("SHOW CATALOGS")
display(all_catalogs_df)

# COMMAND ----------

# Apply catalog filter
if CATALOG_FILTER == "*":
    catalogs = [
        row.catalog
        for row in all_catalogs_df.collect()
        if row.catalog not in EXCLUDE_CATALOGS
    ]
else:
    catalogs = [
        c.strip()
        for c in CATALOG_FILTER.split(",")
        if c.strip() not in EXCLUDE_CATALOGS
    ]

print(f"Scanning {len(catalogs)} catalog(s): {catalogs}")

# COMMAND ----------

# Enumerate all schemas across selected catalogs
schema_rows = []
for catalog in catalogs:
    try:
        schemas_df = spark.sql(f"SHOW SCHEMAS IN `{catalog}`")
        for row in schemas_df.collect():
            schema_name = row.databaseName
            if SCHEMA_FILTER == "*" or schema_name in [s.strip() for s in SCHEMA_FILTER.split(",")]:
                schema_rows.append((catalog, schema_name))
    except Exception as e:
        print(f"WARNING: Cannot list schemas in '{catalog}': {e}")

schemas_df = spark.createDataFrame(schema_rows, ["catalog_name", "schema_name"])
print(f"Found {schemas_df.count()} schema(s)")
display(schemas_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Table and View Inventory

# COMMAND ----------

def get_tables_for_schema(catalog: str, schema: str) -> list:
    """List all tables/views in a schema with basic metadata."""
    results = []
    try:
        tables_df = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`")
        for row in tables_df.collect():
            table_name = row.tableName
            is_temp = row.isTemporary if hasattr(row, "isTemporary") else False
            full_name = f"`{catalog}`.`{schema}`.`{table_name}`"

            # Get table type and metadata via DESCRIBE EXTENDED
            try:
                desc_df = spark.sql(f"DESCRIBE TABLE EXTENDED {full_name}")
                desc_map = {
                    r.col_name.strip(): r.data_type.strip()
                    for r in desc_df.collect()
                    if r.col_name and r.data_type
                }

                table_type = desc_map.get("Type", "UNKNOWN")
                provider = desc_map.get("Provider", "UNKNOWN")
                owner = desc_map.get("Owner", "")
                location = desc_map.get("Location", "")
                created_at = desc_map.get("Created Time", "")
                last_access = desc_map.get("Last Access", "")
                comment = desc_map.get("Comment", "")

                # Extract columns (rows before the empty separator line)
                columns = []
                for r in desc_df.collect():
                    if r.col_name.strip() == "" or r.col_name.startswith("#"):
                        break
                    columns.append(f"{r.col_name} ({r.data_type})")

            except Exception:
                table_type = "UNKNOWN"
                provider = ""
                owner = ""
                location = ""
                created_at = ""
                last_access = ""
                comment = ""
                columns = []

            results.append({
                "catalog_name": catalog,
                "schema_name": schema,
                "table_name": table_name,
                "full_name": f"{catalog}.{schema}.{table_name}",
                "table_type": table_type,
                "provider": provider,
                "owner": owner,
                "location": location,
                "created_at": created_at,
                "last_access": last_access,
                "comment": comment,
                "column_count": len(columns),
                "columns": ", ".join(columns),
                "is_temporary": is_temp,
            })
    except Exception as e:
        print(f"WARNING: Cannot list tables in '{catalog}.{schema}': {e}")

    return results

# COMMAND ----------

# Collect table inventory across all schemas
all_tables = []
schema_list = schemas_df.collect()

for row in schema_list:
    tables = get_tables_for_schema(row.catalog_name, row.schema_name)
    all_tables.extend(tables)

if all_tables:
    inventory_df = spark.createDataFrame(all_tables)
    inventory_df = inventory_df.select(
        "catalog_name", "schema_name", "table_name", "full_name",
        "table_type", "provider", "owner", "location",
        "created_at", "last_access", "comment",
        "column_count", "columns", "is_temporary"
    )
else:
    inventory_df = spark.createDataFrame(
        [],
        "catalog_name STRING, schema_name STRING, table_name STRING, full_name STRING, "
        "table_type STRING, provider STRING, owner STRING, location STRING, "
        "created_at STRING, last_access STRING, comment STRING, "
        "column_count LONG, columns STRING, is_temporary BOOLEAN"
    )

print(f"Found {inventory_df.count()} table(s)/view(s)")
display(inventory_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Row Counts
# MAGIC
# MAGIC Collect row counts for each table. This can be slow for large catalogs —
# MAGIC disable via the `collect_row_counts` widget.

# COMMAND ----------

def safe_row_count(full_name: str) -> tuple:
    """Return (full_name, row_count) or (full_name, -1) on error."""
    try:
        count = spark.table(full_name).count()
        return (full_name, count)
    except Exception:
        return (full_name, -1)

# COMMAND ----------

if COLLECT_ROW_COUNTS and inventory_df.count() > 0:
    table_names = [
        row.full_name
        for row in inventory_df.where(
            F.col("table_type").isin("MANAGED", "EXTERNAL", "TABLE")
            | F.col("table_type").contains("TABLE")
        ).select("full_name").collect()
    ]

    print(f"Collecting row counts for {len(table_names)} table(s)...")

    row_count_results = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(safe_row_count, name): name for name in table_names}
        for i, future in enumerate(as_completed(futures)):
            full_name, count = future.result()
            row_count_results.append((full_name, count))
            if (i + 1) % 25 == 0:
                print(f"  Progress: {i + 1}/{len(table_names)}")

    row_counts_df = spark.createDataFrame(row_count_results, ["full_name", "row_count"])
    inventory_df = inventory_df.join(row_counts_df, on="full_name", how="left")

    display(
        inventory_df
        .where(F.col("row_count").isNotNull())
        .orderBy(F.col("row_count").desc())
        .select("full_name", "table_type", "column_count", "row_count", "owner")
    )
else:
    inventory_df = inventory_df.withColumn("row_count", F.lit(None).cast(LongType()))
    if not COLLECT_ROW_COUNTS:
        print("Row count collection disabled. Enable via the collect_row_counts widget.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Storage Statistics (Delta Tables)
# MAGIC
# MAGIC Collect size, file count, and partitioning info via `DESCRIBE DETAIL`.

# COMMAND ----------

def safe_describe_detail(full_name: str) -> dict:
    """Return storage stats from DESCRIBE DETAIL or defaults on error."""
    try:
        detail = spark.sql(f"DESCRIBE DETAIL `{full_name}`").collect()[0]
        return {
            "full_name": full_name,
            "size_bytes": detail.sizeInBytes if hasattr(detail, "sizeInBytes") else None,
            "num_files": detail.numFiles if hasattr(detail, "numFiles") else None,
            "partition_columns": ", ".join(detail.partitionColumns) if hasattr(detail, "partitionColumns") and detail.partitionColumns else "",
            "table_properties": str(detail.properties) if hasattr(detail, "properties") else "",
            "created_at_detail": str(detail.createdAt) if hasattr(detail, "createdAt") else "",
            "last_modified": str(detail.lastModified) if hasattr(detail, "lastModified") else "",
            "min_reader_version": detail.minReaderVersion if hasattr(detail, "minReaderVersion") else None,
            "min_writer_version": detail.minWriterVersion if hasattr(detail, "minWriterVersion") else None,
        }
    except Exception:
        return {
            "full_name": full_name,
            "size_bytes": None,
            "num_files": None,
            "partition_columns": "",
            "table_properties": "",
            "created_at_detail": "",
            "last_modified": "",
            "min_reader_version": None,
            "min_writer_version": None,
        }

# COMMAND ----------

if COLLECT_STORAGE_STATS and inventory_df.count() > 0:
    delta_tables = [
        row.full_name
        for row in inventory_df.where(
            F.lower(F.col("provider")).isin("delta", "hive")
            | F.col("table_type").isin("MANAGED", "EXTERNAL")
            | F.col("table_type").contains("TABLE")
        ).select("full_name").collect()
    ]

    print(f"Collecting storage stats for {len(delta_tables)} table(s)...")

    storage_results = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(safe_describe_detail, name): name for name in delta_tables}
        for i, future in enumerate(as_completed(futures)):
            storage_results.append(future.result())
            if (i + 1) % 25 == 0:
                print(f"  Progress: {i + 1}/{len(delta_tables)}")

    if storage_results:
        storage_df = spark.createDataFrame(storage_results)

        # Add human-readable size
        storage_df = storage_df.withColumn(
            "size_mb", F.round(F.col("size_bytes") / (1024 * 1024), 2)
        ).withColumn(
            "size_gb", F.round(F.col("size_bytes") / (1024 * 1024 * 1024), 4)
        )

        inventory_df = inventory_df.join(
            storage_df, on="full_name", how="left"
        )

        display(
            storage_df
            .where(F.col("size_bytes").isNotNull())
            .orderBy(F.col("size_bytes").desc())
            .select("full_name", "size_mb", "size_gb", "num_files", "partition_columns", "last_modified")
        )
else:
    if not COLLECT_STORAGE_STATS:
        print("Storage stats collection disabled. Enable via the collect_storage_stats widget.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Volume, Function, and Model Inventory

# COMMAND ----------

# Volumes
volume_rows = []
for row in schema_list:
    try:
        vols = spark.sql(f"SHOW VOLUMES IN `{row.catalog_name}`.`{row.schema_name}`")
        for v in vols.collect():
            volume_rows.append({
                "catalog_name": row.catalog_name,
                "schema_name": row.schema_name,
                "volume_name": v.volume_name if hasattr(v, "volume_name") else str(v[0]),
                "volume_type": v.volume_type if hasattr(v, "volume_type") else "",
            })
    except Exception:
        pass

if volume_rows:
    volumes_df = spark.createDataFrame(volume_rows)
    print(f"Found {volumes_df.count()} volume(s)")
    display(volumes_df)
else:
    print("No volumes found.")

# COMMAND ----------

# Functions (UDFs)
function_rows = []
for row in schema_list:
    try:
        funcs = spark.sql(f"SHOW FUNCTIONS IN `{row.catalog_name}`.`{row.schema_name}`")
        for f_row in funcs.collect():
            func_name = f_row.function if hasattr(f_row, "function") else str(f_row[0])
            # Skip built-in functions
            if "." in func_name:
                function_rows.append({
                    "catalog_name": row.catalog_name,
                    "schema_name": row.schema_name,
                    "function_name": func_name,
                })
    except Exception:
        pass

if function_rows:
    functions_df = spark.createDataFrame(function_rows)
    print(f"Found {functions_df.count()} user function(s)")
    display(functions_df)
else:
    print("No user functions found.")

# COMMAND ----------

# Registered ML Models
model_rows = []
for row in schema_list:
    try:
        models = spark.sql(f"SHOW MODELS IN `{row.catalog_name}`.`{row.schema_name}`")
        for m in models.collect():
            model_rows.append({
                "catalog_name": row.catalog_name,
                "schema_name": row.schema_name,
                "model_name": m.name if hasattr(m, "name") else str(m[0]),
            })
    except Exception:
        pass

if model_rows:
    models_df = spark.createDataFrame(model_rows)
    print(f"Found {models_df.count()} registered model(s)")
    display(models_df)
else:
    print("No registered models found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Inventory Summary

# COMMAND ----------

# Overall summary
summary_data = {
    "catalogs": len(catalogs),
    "schemas": len(schema_list),
    "tables_and_views": inventory_df.count(),
    "tables": inventory_df.where(
        F.col("table_type").isin("MANAGED", "EXTERNAL") | F.col("table_type").contains("TABLE")
    ).count(),
    "views": inventory_df.where(
        F.col("table_type").contains("VIEW")
    ).count(),
    "volumes": len(volume_rows),
    "functions": len(function_rows),
    "models": len(model_rows),
}

summary_df = spark.createDataFrame(
    [Row(metric=k, count=v) for k, v in summary_data.items()]
)
display(summary_df)

# COMMAND ----------

# Breakdown by catalog and schema
schema_summary = (
    inventory_df
    .groupBy("catalog_name", "schema_name")
    .agg(
        F.count("*").alias("total_objects"),
        F.sum(F.when(
            F.col("table_type").isin("MANAGED", "EXTERNAL") | F.col("table_type").contains("TABLE"), 1
        ).otherwise(0)).alias("tables"),
        F.sum(F.when(F.col("table_type").contains("VIEW"), 1).otherwise(0)).alias("views"),
        F.sum("column_count").alias("total_columns"),
        F.avg("column_count").cast("int").alias("avg_columns_per_table"),
    )
    .orderBy("catalog_name", "schema_name")
)

# Add row count and size totals if collected
if COLLECT_ROW_COUNTS and "row_count" in inventory_df.columns:
    rc_summary = (
        inventory_df
        .where(F.col("row_count") >= 0)
        .groupBy("catalog_name", "schema_name")
        .agg(
            F.sum("row_count").alias("total_rows"),
            F.max("row_count").alias("max_table_rows"),
        )
    )
    schema_summary = schema_summary.join(rc_summary, on=["catalog_name", "schema_name"], how="left")

if COLLECT_STORAGE_STATS and "size_bytes" in inventory_df.columns:
    size_summary = (
        inventory_df
        .where(F.col("size_bytes").isNotNull())
        .groupBy("catalog_name", "schema_name")
        .agg(
            F.round(F.sum("size_bytes") / (1024 * 1024 * 1024), 2).alias("total_size_gb"),
            F.sum("num_files").alias("total_files"),
        )
    )
    schema_summary = schema_summary.join(size_summary, on=["catalog_name", "schema_name"], how="left")

display(schema_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Full Inventory Table
# MAGIC
# MAGIC Complete inventory with all collected metrics. Optionally persist to a Delta table
# MAGIC for historical tracking.

# COMMAND ----------

# Add a snapshot timestamp
final_inventory = inventory_df.withColumn("snapshot_timestamp", F.current_timestamp())

display(
    final_inventory.orderBy("catalog_name", "schema_name", "table_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Uncomment the cell below to persist the inventory as a Delta table for historical tracking.

# COMMAND ----------

# # Persist inventory to a Delta table
# OUTPUT_TABLE = "main.observability.catalog_inventory"
#
# (
#     final_inventory
#     .write
#     .mode("append")
#     .option("mergeSchema", "true")
#     .saveAsTable(OUTPUT_TABLE)
# )
#
# print(f"Inventory snapshot saved to {OUTPUT_TABLE}")
