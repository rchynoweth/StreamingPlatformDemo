# Databricks notebook source
# MAGIC %pip install git+https://github.com/rchynoweth/StreamingPlatformDemo.git@main

# COMMAND ----------

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
                  get_json_object(col("value").cast("string"), '$.time'),
                  get_json_object(col("value").cast("string"), '$.action')
                  )
          )


# COMMAND ----------


