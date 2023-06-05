# Databricks notebook source
# MAGIC %md # Load Test Notebook - Data Generation
# MAGIC 
# MAGIC There are two options for generating data. 
# MAGIC 1. Load json data into a batch dataframe and use a loop to continuously write json records to storage
# MAGIC 1. Additionally, read the json records (output of the one above) and write to Kafka. 

# COMMAND ----------

# MAGIC %pip install git+https://github.com/rchynoweth/StreamingPlatformDemo.git@main

# COMMAND ----------

import time, uuid, random
from pyspark.sql.functions import *
from dlt_platform.connectors.file_source_connect import FileSourceConnect
from dlt_platform.connectors.kafka_connect import KafkaConnect

# COMMAND ----------

sleep_time = 0 # seconds 
n_loads = 100 


input_dir = '/databricks-datasets/structured-streaming/events/'

## File Params
output_dir = '/dlt_platform_test/streaming/events'
json_schema_location = '/dlt_platform_test/streaming_schemas/events'

# COMMAND ----------

# dbutils.fs.rm(output_dir, True) # only run if needed. Good to have a lot of data with streaming. 

# COMMAND ----------

print(f"---- Num Rows: {spark.read.json(input_dir).count()}")
print(f"---- Num Files: {len(dbutils.fs.ls(input_dir))}")
print(f"---- Apprx Rows Per File: {spark.read.json(input_dir).count()/len(dbutils.fs.ls(input_dir))}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## File Source Data Generation

# COMMAND ----------

file_source_client = FileSourceConnect()

# COMMAND ----------

df = file_source_client.batch_read_files(spark, input_path=input_dir, file_type='json')
df = (df
      .withColumn("time_datetime", from_unixtime(col('time')))
      .withColumn("FileWriteDatetime", current_timestamp())
)
display(df)
# df = file_source_client.read_file_stream(spark, input_path=input_dir, file_type='json', schema_location=json_schema_location)
# df = file_source_client.read_file_stream_load_test(spark, input_path=input_dir, file_type='json', schema_location=json_schema_location)

# COMMAND ----------

def write_data(df, output_path, file_type='json', mode='append', multiple=5):
  for j in range(0, multiple):
    df = df.union(df)
  # write data to files 
  for i in range(0, n_loads):
    (
        df.write
        .format(file_type)
        .mode(mode)
        .save(output_path)
    )
    time.sleep(sleep_time)

# COMMAND ----------

write_data(df, output_path=output_dir)

# COMMAND ----------

print(f"---- Num Rows: {spark.read.json(output_dir).count()}")
print(f"---- Num Files: {len(dbutils.fs.ls(output_dir))}")
print(f"---- Apprx Rows Per File: {spark.read.json(output_dir).count()/len(dbutils.fs.ls(output_dir))}")
