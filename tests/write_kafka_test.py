# Databricks notebook source
# MAGIC %md # Load Test Notebook - Write to Kafka 
# MAGIC
# MAGIC There are two options for generating data. 
# MAGIC 1. Load json data into a batch dataframe and use a loop to continuously write json records to storage
# MAGIC 1. Additionally, read the json records (output of the one above) and write to Kafka. 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %pip install git+https://github.com/rchynoweth/StreamingPlatformDemo.git@main

# COMMAND ----------

import time, uuid, random
from pyspark.sql.functions import *
from dlt_platform.connectors.file_source_connect import FileSourceConnect
from dlt_platform.connectors.kafka_connect import KafkaConnect

# COMMAND ----------

input_dir = '/dlt_platform_test/streaming/events'
## Kafka Params
schema_location = '/dlt_platform_test/streaming_schemas/events_kafka'
checkpoint_location = '/dlt_platform_test/streaming_checkpoints/events_kafka'
topic_name = 'ryan_chynoweth_kafka_topic'
bootstrap_servers = dbutils.secrets.get('oetrta','msk_plain_text') 

# COMMAND ----------

# print(f"---- Num Rows: {spark.read.json(input_dir).countApprox()}")
print(f"---- Num Files: {len(dbutils.fs.ls(input_dir))}")

# COMMAND ----------

file_source_client = FileSourceConnect()

# COMMAND ----------

uuidUdf= udf(lambda : uuid.uuid4().hex,StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Kafka Source Data Generation

# COMMAND ----------

kafka_client = KafkaConnect(bootstrap_servers)

# COMMAND ----------

streamDF = (file_source_client.read_file_stream_load_test(spark, input_path=input_dir, file_type='json', schema_location=schema_location, max_files=5)
            .withColumn("kafkaWriteTime", current_timestamp())
            .withColumn('key', uuidUdf())
          )

# COMMAND ----------

dbutils.fs.rm(checkpoint_location, True)

# COMMAND ----------

value_columns = ['FileWriteDatetime', 'action', 'time', 'time_datetime', 'kafkaWriteTime']
kafka_client.write_kafka_stream_plaintext(streamDF, key_col='key', value_cols=value_columns, topic=topic_name, checkpoint_location=checkpoint_location)

# COMMAND ----------


