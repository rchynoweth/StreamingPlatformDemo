# Databricks notebook source
# MAGIC %pip install git+https://github.com/rchynoweth/StreamingPlatformDemo.git@main

# COMMAND ----------

import dlt 
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Custom Python Library - i.e. "template"
from dlt_platform.connectors.kafka_connect import KafkaConnect
from dlt_platform.connectors.file_source_connect import FileSourceConnect
from dlt_platform.connectors.jdbc_connect import JDBCConnect

# COMMAND ----------

secret_scope = ''
kafka_servers = dbutils.secrets.get(secret_scope, "kafka-bootstrap-servers-plaintext")
topic = 'ryan_chynoweth_kafka_test'

# COMMAND ----------

k = KafkaConnect(kafka_servers)
f = FileSourceConnect()

# COMMAND ----------

# DBTITLE 1,Creates a flow from Kafka source
@dlt.table(name='kafka_events')
def kafka_events():
  return ( k.read_kafka_stream(spark=spark,topic=topic) )


# COMMAND ----------

# DBTITLE 1,Creates a flow from streaming file source
@dlt.table(name='test_file_events')
def test_file_events():
  return f.read_file_stream(spark, '/databricks-datasets/structured-streaming/events', 'json')

# COMMAND ----------


