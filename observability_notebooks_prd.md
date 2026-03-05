**Product Requirements Document: Unity Catalog Observability Notebooks**

**1. Objective and Scope**
The goal of this product is to develop a suite of Databricks notebooks that track comprehensive data observability across an organization's lakehouse. By leveraging Unity Catalog and Databricks system tables, these notebooks will monitor data quality, cost, pipeline performance, streaming health, and data lineage,,. 

**2. Target Audience**
*   **Data Engineers:** Require visibility into streaming backlogs, pipeline bottlenecks, and operational health to troubleshoot job failures,.
*   **Data Stewards and Analysts:** Need to track data quality (SLAs/SLOs), identify missing values, and analyze data lineage to ensure they are making decisions on trustworthy data,,.
*   **Platform / Metastore Administrators:** Focus on rationalizing costs, monitoring system billing, and enforcing data governance across workspaces,.

**3. Prerequisites and Architecture**
*   **Unity Catalog:** The workspace must have Unity Catalog enabled, as system tables and lineage features rely exclusively on this centralized governance store,.
*   **System Tables:** An account admin must enable the `system.lakeflow`, `system.billing`, and `system.access` schemas,. 
*   **Permissions:** Users interacting with the notebooks must be metastore admins, account admins, or have explicit `USE` and `SELECT` permissions on the necessary system schemas. Users must also have at least `BROWSE` permissions on target tables to view their lineage,.

---

**4. Notebook Specifications**

**Notebook 1: Cost and Operational Health Observability**
*   **Purpose:** Track job costs, identify the most expensive pipelines, and monitor retry/failure patterns to optimize Databricks spend,.
*   **Data Sources:** `system.billing.usage`, `system.billing.list_prices`, `system.lakeflow.jobs`, and `system.lakeflow.job_run_timeline`,.
*   **Required Outputs:**
    *   **Spend Trend Analysis:** Calculate 7-day and 14-day spend growth to identify sudden spikes in job costs.
    *   **Most Expensive Jobs:** Aggregate the total list cost per job run over the last 30 days,.
    *   **Failed Jobs and Retry Patterns:** Calculate success ratios, failure costs, and frequent repair runs (where `result_state` is 'ERROR', 'FAILED', or 'TIMED_OUT'),.

**Notebook 2: Data Quality and Lakehouse Monitoring**
*   **Purpose:** Measure statistical distribution, track data drift, and monitor validation constraints (expectations) at runtime,,.
*   **Data Sources:** Delta Live Tables (DLT) event logs and Lakehouse Monitoring metric tables,.
*   **Required Outputs:**
    *   **Expectation Tracking:** Query the `event_log()` table-valued function to parse JSON strings and extract `passed_records` and `failed_records` for specific data quality rules,,.
    *   **Quality Summaries:** Calculate the average pass and fail rates of data expectations across the pipeline.
    *   **Statistical Profiling:** Use Snapshot or Time Series profiles to capture minimum, maximum, mean, standard deviation, and missing value counts (null rows) for critical columns,,.

**Notebook 3: Data Lineage and Impact Analysis**
*   **Purpose:** Trace the upstream origins of datasets and understand the downstream impact of potential data issues or schema changes,.
*   **Data Sources:** `system.access.table_lineage` and `system.access.column_lineage`. (Fallback: Use the Data Lineage REST API `/api/2.0/lineage-tracking/table-lineage` if querying a region that doesn't support the system tables).
*   **Required Outputs:**
    *   **Upstream/Downstream Tracking:** Query connections to map parent and child tables, identifying notebooks and jobs linked to the transformation,,.
    *   **Column-Level Tracing:** Identify how specific fields are derived to prevent compliance violations or data leakages,.

**Notebook 4: Streaming and Pipeline Performance**
*   **Purpose:** Ensure streaming applications maintain data freshness and high throughput without falling behind,.
*   **Data Sources:** DLT `event_log` (filtering by `event_type = 'flow_progress'`) and `StreamingQueryListener` metrics,.
*   **Required Outputs:**
    *   **Throughput & Duration:** Calculate the average batch duration in seconds and the average throughput in rows per second,.
    *   **Backlog Monitoring:** Track `backlog_bytes` and `backlog_files` to proactively tune Kafka/Kinesis off-sets or Auto Loader instances before downstream systems are impacted,.
    *   **Resource Utilization:** Monitor the average number of task slots and queued tasks to optimize compute scaling.

**Notebook 5: Custom Application Logging & Alerting Framework**
*   **Purpose:** Standardize python-based error handling and log aggregation for tasks outside of DLT or system tables,.
*   **Implementation Requirements:**
    *   Use Python’s `logging` module to capture `DEBUG`, `INFO`, `WARNING`, `ERROR`, and `CRITICAL` levels.
    *   Implement **Structured JSON Logging** with ISO-8601 timestamps (`%Y-%m-%dT%H:%M:%S`) to allow easy parsing and chronological sorting,.
    *   Log files must be written to centralized storage via a `FileHandler` pointing to a DBFS path (e.g., `/dbfs/FileStore/logs/`).
    *   Implement `try-except` blocks around transformations to cleanly capture and log exceptions.

---

**5. Non-Functional Requirements**
*   **Alerting Integration:** The notebooks should support integration with Databricks SQL Alerts or Workflows to trigger notifications (via Email, Slack, Teams, or PagerDuty webhooks) when costs spike, jobs time out, or data quality rules are breached,,.
*   **Deployment & Automation:** All observability notebooks should be treated as Infrastructure as Code (IaC) and deployed across development, staging, and production workspaces using **Databricks Asset Bundles (DABs)** or the **Databricks Terraform Provider**,. 
*   **Security:** No sensitive Personal Identifiable Information (PII) should be captured in the logs. Dynamic views or column-masking functions (e.g., `is_account_group_member()`) should be used if stakeholders outside of the data engineering team need access to raw pipeline outputs,.
