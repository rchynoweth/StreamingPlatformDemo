# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Custom Python Library - i.e. "template"
from dlt_platform.connectors.kafka_connect import KafkaConnect

# COMMAND ----------

secret_scope = ''
kafka_servers = dbutils.secrets.get(secret_scope, "kafka-bootstrap-servers-plaintext")

topic = 'ryan_chynoweth_kafka_test2'
checkpoint_location = '/home/ryan.chynoweth@databricks.com/kafka_test/kafka_checkpoint'
dbutils.fs.rm(checkpoint_location, True)

# COMMAND ----------

k = KafkaConnect(kafka_servers)

# COMMAND ----------

import uuid

uuidUdf= udf(lambda : uuid.uuid4().hex,StringType())

input_path = "/databricks-datasets/structured-streaming/events"
input_schema = spark.read.json(input_path).schema

input_stream = (spark
                .readStream.format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("maxFilesPerTrigger", 1)
                .option("cloudFiles.schemaLocation", "/ryan/schema")
                .load(input_path)
                .withColumn("eventId", uuidUdf())
               )

# COMMAND ----------

# DBTITLE 1,Write to Kafka
k.write_kafka_stream(df=input_stream, key_col='eventId', value_cols=['time', 'action'], topic = topic, checkpoint_location=checkpoint_location)

# COMMAND ----------

# DBTITLE 1,Read from Kafka
df = k.read_kafka_stream(spark=spark,topic=topic)
display(df.select(col('key').cast('string'), col("value").cast("string")))
