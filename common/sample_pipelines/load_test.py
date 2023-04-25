# Databricks notebook source
# MAGIC %md # Load Test Notebook 

# COMMAND ----------

# MAGIC %pip install git+https://github.com/rchynoweth/StreamingTemplates.git@main

# COMMAND ----------

import time 
from pyspark.sql.functions import current_timestamp
from dlt_platform.connectors.jdbc_connect import JDBCConnect

# COMMAND ----------

sleep_time = 2 # 2 seconds 
n_loads = 1000 
scope_name = ''
output_dir = ''
user = dbutils.secrets.get(scope_name, '')
pw = dbutils.secrets.get(scope_name, '')
url = dbutils.secrets.get(scope_name, '')

# COMMAND ----------

j = JDBCConnect(jdbcUsername=user, jdbcPassword=pw, jdbcUrl=url)

# COMMAND ----------

query = ""

# COMMAND ----------

def load_data(output_path, file_type='json', mode='append'):
    # load data 
    df = (
        j.read_jdbc_query(spark=spark, sql_query=query)
        .withColumn('database_load_datetime', current_timestamp())
    )

    # write data to files 
    (
        df.write
        .format(file_type)
        .mode(mode)
        .save(output_path)
    )

# COMMAND ----------

for i in range(0, n_loads):
    load_data(output_path=output_dir)
    time.sleep(sleep_time)

# COMMAND ----------
