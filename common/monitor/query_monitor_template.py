# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Live Tables - The Event Log
# MAGIC 
# MAGIC Tips and Best Practices:
# MAGIC - Centralize Delta Live Table Pipelines in a single storage location
# MAGIC   - Let's assume you have a pipeline, `demo_dlt_pipeline` and you have an Azure Data Lake Storage Gen2 account and container respectfully named: `adlsstorage` and `adlscontainer`. Then please use the following URI: `abfss://adlscontainer@adlsstorage.dfs.core.windows.net/dlt_pipelines/demo_dlt_pipeline`. 
# MAGIC   - This allows you to easily map the pipeline to the folder by name. 
# MAGIC   - If you have another pipeline, `demo_dlt_pipeline2` then the URI would be: `abfss://adlscontainer@adlsstorage.dfs.core.windows.net/dlt_pipelines/demo_dlt_pipeline2`. 
# MAGIC   - This allows you to read both event logs with `spark.read.load("abfss://adlscontainer@adlsstorage.dfs.core.windows.net/dlt_pipelines/*/system/events")`. 
# MAGIC   - Please note that the `dlt_pipielines` directory can be named anything you wish and can have any number of subdirectories.
# MAGIC   - Do not put a pipeline location at the container base directory and always provide an external location. 
# MAGIC   - [Public Resources](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-event-log.html#data-quality)

# COMMAND ----------

dbutils.widgets.text("schema_name", "")
schema_name = dbutils.widgets.get("schema_name")

# COMMAND ----------

spark.sql(f"use {schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raw Data 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw_event_log

# COMMAND ----------

# MAGIC %md
# MAGIC ## User Action Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from user_action_details

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Expectation Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct * from flow_progress_expectations

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (
# MAGIC select pipeline_id
# MAGIC , pipeline_name
# MAGIC , date_trunc("hour", timestamp) as hourly_timestamp
# MAGIC , date
# MAGIC , flow_id
# MAGIC , flow_name
# MAGIC , expectation_name
# MAGIC , dataset
# MAGIC , passed_records
# MAGIC , failed_records
# MAGIC 
# MAGIC from flow_progress_expectations 
# MAGIC )
# MAGIC 
# MAGIC select pipeline_id
# MAGIC , pipeline_name
# MAGIC , hourly_timestamp
# MAGIC , date
# MAGIC , flow_id
# MAGIC , flow_name
# MAGIC , expectation_name
# MAGIC , dataset
# MAGIC , sum(passed_records) as passed_records
# MAGIC , sum(failed_records) as failed_records
# MAGIC 
# MAGIC 
# MAGIC from cte
# MAGIC 
# MAGIC group by pipeline_id
# MAGIC , pipeline_name
# MAGIC , hourly_timestamp
# MAGIC , date
# MAGIC , flow_id
# MAGIC , flow_name
# MAGIC , expectation_name
# MAGIC , dataset

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hourly_expectations_agg

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cluster Resource Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cluster_resource_details

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (
# MAGIC select pipeline_id
# MAGIC , pipeline_name
# MAGIC , date_trunc("hour", timestamp) as hourly_timestamp
# MAGIC , summary_duration_ms
# MAGIC , num_task_slots
# MAGIC , avg_num_task_slots
# MAGIC , avg_task_slot_utilization
# MAGIC , num_executors
# MAGIC , avg_num_queued_tasks
# MAGIC 
# MAGIC 
# MAGIC from cluster_resource_details 
# MAGIC )
# MAGIC select pipeline_id
# MAGIC , pipeline_name
# MAGIC , hourly_timestamp
# MAGIC , avg(summary_duration_ms)/1000 as avg_summary_duration_seconds
# MAGIC , max(num_task_slots) as max_task_slots
# MAGIC , min(num_task_slots) as min_task_slots
# MAGIC , median(avg_num_task_slots) as median_avg_num_task_slots
# MAGIC , median(avg_task_slot_utilization) as median_avg_task_slot_utilization
# MAGIC , max(num_executors) as max_num_executors
# MAGIC , min(num_executors) as min_num_executors
# MAGIC , avg(num_executors) as avg_num_executors
# MAGIC , median(num_executors) as median_num_executors
# MAGIC , median(avg_num_queued_tasks) as median_avg_num_queued_tasks
# MAGIC 
# MAGIC 
# MAGIC from cte
# MAGIC 
# MAGIC group by pipeline_id
# MAGIC , pipeline_name
# MAGIC , hourly_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flow Progress Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from daily_flow_agg

# COMMAND ----------


