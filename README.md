# Unity Catalog Observability Notebooks

A suite of Databricks notebooks for comprehensive data observability across your lakehouse. Leverages Unity Catalog system tables to monitor costs, data quality, lineage, streaming health, and application logging.

## Prerequisites

- **Unity Catalog** enabled on your workspace
- **System tables** enabled by an account admin: `system.lakeflow`, `system.billing`, `system.access`
- **Permissions**: Metastore admin, account admin, or explicit `USE`/`SELECT` on required system schemas. `BROWSE` on target tables for lineage features.

## Notebooks

### 00 — Catalog Inventory

Enumerates all catalogs, schemas, tables, views, volumes, functions, and registered ML models in your Unity Catalog metastore. Collects per-table statistics including row counts, column listings, storage size, file counts, and partitioning info.

**Key features:**
- Configurable catalog/schema filters and exclusion lists
- Parallel row count and `DESCRIBE DETAIL` collection (toggleable for performance)
- Per-schema summary with total rows, storage size, and object counts
- Optional persistence to a Delta table for historical tracking

### 01 — Cost and Operational Health

Tracks job costs, identifies the most expensive pipelines, and monitors retry/failure patterns.

**Data sources:** `system.billing.usage`, `system.billing.list_prices`, `system.lakeflow.jobs`, `system.lakeflow.job_run_timeline`

**Key outputs:**
- 7-day and 14-day rolling spend with growth rate percentages
- Top 25 most expensive jobs over the last 30 days
- Job success ratios, wasted compute hours, and failure timelines
- `cost_alerts` view for SQL Alert integration

### 02 — Data Quality and Lakehouse Monitoring

Measures statistical distribution, tracks data drift, and monitors DLT expectation pass/fail rates.

**Data sources:** DLT `event_log()` table-valued function, Lakehouse Monitoring profile and drift metric tables

**Key outputs:**
- Per-expectation `passed_records` / `failed_records` extraction from DLT event log JSON
- Average pass/fail rates aggregated across pipeline runs
- Statistical profiles: min, max, mean, stddev, null counts
- Drift detection comparing current metrics against baseline
- `quality_alerts` view for expectations exceeding a 5% failure threshold

### 03 — Data Lineage and Impact Analysis

Traces upstream origins and downstream dependencies of datasets for impact analysis before schema changes or data fixes.

**Data sources:** `system.access.table_lineage`, `system.access.column_lineage`, Data Lineage REST API (fallback)

**Key outputs:**
- Direct upstream and downstream table mappings
- Recursive multi-hop lineage traversal (up to 5 hops)
- Column-level lineage tracing for compliance and data leakage prevention
- REST API fallback (`/api/2.0/lineage-tracking/table-lineage`) for unsupported regions

### 04 — Streaming and Pipeline Performance

Monitors streaming applications for data freshness, throughput, backlog growth, and resource utilization.

**Data sources:** DLT `event_log` (filtered by `event_type = 'flow_progress'`), optional `StreamingQueryListener` metrics table

**Key outputs:**
- Avg/min/max/p95 batch duration and rows-per-second throughput
- `backlog_bytes` / `backlog_files` tracking with daily growth trends
- Task slot utilization and queued task monitoring
- Optional `StreamingQueryListener` metrics integration
- `streaming_alerts` view with backlog (>1 GB critical) and utilization (>90%) thresholds

### 05 — Custom Application Logging and Alerting

Standardizes Python-based error handling and log aggregation for workloads outside of DLT or system tables.

**Key features:**
- `StructuredJsonFormatter` — JSON log records with ISO-8601 timestamps
- `RotatingFileHandler` writing to a configurable DBFS path
- `log_operation` context manager and `@log_transform` decorator with automatic duration tracking and exception capture
- Log analysis by reading JSON logs back as a DataFrame
- `PiiSafeFormatter` — automatic redaction of emails, SSNs, phone numbers, and credit card numbers
- `log_alerts` view categorizing errors as OK / WARN / ALERT / PAGE

## Alerting Integration

Each notebook (01–05) produces temporary views designed for use with [Databricks SQL Alerts](https://docs.databricks.com/sql/user/alerts/index.html). These can trigger notifications via email, Slack, Teams, or PagerDuty webhooks when:
- Spend growth exceeds configured thresholds
- Job failure rates spike
- Data quality expectations are breached
- Streaming backlogs grow beyond safe limits
- Application error rates increase

## Deployment

All notebooks are Databricks-format `.py` source files, compatible with:
- [Databricks Asset Bundles (DABs)](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Databricks Terraform Provider](https://registry.terraform.io/providers/databricks/databricks/latest/docs)

Notebooks use `dbutils.widgets` for runtime parameterization, making them suitable for scheduled Workflows across dev, staging, and production workspaces.
