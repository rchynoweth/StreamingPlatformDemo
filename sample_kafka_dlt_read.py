# Databricks notebook source
import dlt

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Custom Python Library - i.e. "template"
from dlt_platform.connectors.kafka_connect import KafkaConnect

# COMMAND ----------

secret_scope = 'oetrta'
kafka_servers = dbutils.secrets.get(secret_scope, "kafka-bootstrap-servers-plaintext")

topic = 'ryan_chynoweth_kafka_test2'

# COMMAND ----------

k = KafkaConnect(kafka_servers)

# COMMAND ----------

# DBTITLE 1,Read from Kafka
@dlt.table(name='kafka_events')
def kafka_events():
  return (k.read_kafka_stream_plaintext(spark=spark,topic=topic)
          .select(col('key').cast('string'), 
                  col("value").cast("string"),
                  get_json_object(col("value").cast("string"), '$.time').alias('time'),
                  get_json_object(col("value").cast("string"), '$.action').alias('action')
                  )
          # .select(
          #   col("key"),
          #   col("value"),
          #   get_json_object(col("value"), '$.time').alias('time'),
          #   get_json_object(col("value"), '$.action').alias('action')
          # )
          )


# COMMAND ----------


