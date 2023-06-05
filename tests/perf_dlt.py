# Databricks notebook source
# MAGIC %pip install git+https://github.com/rchynoweth/StreamingPlatformDemo.git@main

# COMMAND ----------

# import dlt 
from pyspark.sql.functions import current_timestamp
from dlt_platform.connectors.file_source_connect import FileSourceConnect
from dlt_platform.connectors.kafka_connect import KafkaConnect 
from dlt_platform.connectors.delta_lake_connect import DeltaLakeConnect

# COMMAND ----------

bootstrap_servers = dbutils.secrets.get('oetrta','msk_plain_text') 
topic = 'ryan_chynoweth_kafka_topic'
startingOffsets = "earliest"

# COMMAND ----------

file_source_client = FileSourceConnect()
kafka_client = KafkaConnect(bootstrap_servers)
delta_client = DeltaLakeConnect()

# COMMAND ----------

k_options = {
  "kafka.bootstrap.servers": bootstrap_servers,
  "subscribe": topic,
  "startingOffsets": startingOffsets
}


@dlt.table(name='kafka_ingest_table')
def kafka_ingest_table():
  return (
    kafka_client.generic_read_kafka_stream(spark=spark, options=k_options)
  )

# COMMAND ----------

@dlt.table(name='kakfa_silver')
def kafka_silver():
  df = delta_client.read_stream_delta_table('live.kafka_ingest_table')
  return (
    df.select(col("key").cast("string").alias("eventId"), col("value").cast("string"))
      .withColumn('kafka_silver_datetime', current_timestamp())
  )
