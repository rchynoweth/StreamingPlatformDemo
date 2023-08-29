# Databricks notebook source
from pyspark.sql.functions import unix_timestamp, col, to_timestamp

# COMMAND ----------

spark.sql('use rac_demo_db')

# COMMAND ----------

display(spark.sql('select * from kafka_ingest_table limit 10'))

# COMMAND ----------

df = spark.sql('select * from kafka_silver limit 10')
display(df)

# COMMAND ----------

display(spark.sql('select count(1) from kafka_silver'))

# COMMAND ----------

# DBTITLE 1,Non-Photon
# MAGIC %sql
# MAGIC with cte as (
# MAGIC select
# MAGIC to_timestamp(kafka_silver_datetime) as endTime 
# MAGIC , to_timestamp(kafkaWritetime) as startTime
# MAGIC , unix_timestamp(to_timestamp(kafka_silver_datetime)) - unix_timestamp(to_timestamp(kafkaWritetime)) as diffSeconds
# MAGIC
# MAGIC from kafka_silver
# MAGIC ), cte2 as (
# MAGIC select 
# MAGIC endTime, 
# MAGIC startTime, 
# MAGIC diffSeconds, 
# MAGIC count(1) as recordCount,
# MAGIC count(1)/diffSeconds as rec_per_second
# MAGIC
# MAGIC from cte 
# MAGIC group by endTime, startTime, diffSeconds
# MAGIC order by startTime desc
# MAGIC ) 
# MAGIC
# MAGIC
# MAGIC SELECT 
# MAGIC   endTime, 
# MAGIC   SUM(recordCount) AS total_records,
# MAGIC   UNIX_TIMESTAMP(MAX(endTime)) - UNIX_TIMESTAMP(MIN(startTime)) AS total_seconds,
# MAGIC   SUM(recordCount) / (UNIX_TIMESTAMP(MAX(endTime)) - UNIX_TIMESTAMP(MIN(startTime))) AS records_per_second
# MAGIC
# MAGIC FROM 
# MAGIC   cte2
# MAGIC GROUP BY 
# MAGIC   endTime
# MAGIC

# COMMAND ----------

# DBTITLE 1,Photon - 25 files at a time ~ 180k records a second
# MAGIC %sql
# MAGIC -- 52-55K per second through put. 
# MAGIC with cte as (
# MAGIC select
# MAGIC to_timestamp(kafka_silver_datetime) as endTime 
# MAGIC , to_timestamp(kafkaWritetime) as startTime
# MAGIC , unix_timestamp(to_timestamp(kafka_silver_datetime)) - unix_timestamp(to_timestamp(kafkaWritetime)) as diffSeconds
# MAGIC
# MAGIC from kafka_silver
# MAGIC ), cte2 as (
# MAGIC select 
# MAGIC endTime, 
# MAGIC startTime, 
# MAGIC diffSeconds, 
# MAGIC count(1) as recordCount,
# MAGIC count(1)/diffSeconds as rec_per_second
# MAGIC
# MAGIC from cte 
# MAGIC group by endTime, startTime, diffSeconds
# MAGIC order by startTime desc
# MAGIC ) 
# MAGIC
# MAGIC
# MAGIC SELECT 
# MAGIC   endTime, 
# MAGIC   SUM(recordCount) AS total_records,
# MAGIC   UNIX_TIMESTAMP(MAX(endTime)) - UNIX_TIMESTAMP(MIN(startTime)) AS total_seconds,
# MAGIC   SUM(recordCount) / (UNIX_TIMESTAMP(MAX(endTime)) - UNIX_TIMESTAMP(MIN(startTime))) AS records_per_second
# MAGIC
# MAGIC FROM 
# MAGIC   cte2
# MAGIC GROUP BY 
# MAGIC   endTime
# MAGIC

# COMMAND ----------

# DBTITLE 1,Photon - 5 files - 60k records a second
# MAGIC %sql
# MAGIC -- 35k Records per second 
# MAGIC with cte as (
# MAGIC select
# MAGIC to_timestamp(kafka_silver_datetime) as endTime 
# MAGIC , to_timestamp(kafkaWritetime) as startTime
# MAGIC , unix_timestamp(to_timestamp(kafka_silver_datetime)) - unix_timestamp(to_timestamp(kafkaWritetime)) as diffSeconds
# MAGIC
# MAGIC from kafka_silver
# MAGIC ), cte2 as (
# MAGIC select 
# MAGIC endTime, 
# MAGIC startTime, 
# MAGIC diffSeconds, 
# MAGIC count(1) as recordCount,
# MAGIC count(1)/diffSeconds as rec_per_second
# MAGIC
# MAGIC from cte 
# MAGIC group by endTime, startTime, diffSeconds
# MAGIC order by startTime desc
# MAGIC ) 
# MAGIC
# MAGIC
# MAGIC SELECT 
# MAGIC   endTime, 
# MAGIC   SUM(recordCount) AS total_records,
# MAGIC   UNIX_TIMESTAMP(MAX(endTime)) - UNIX_TIMESTAMP(MIN(startTime)) AS total_seconds,
# MAGIC   SUM(recordCount) / (UNIX_TIMESTAMP(MAX(endTime)) - UNIX_TIMESTAMP(MIN(startTime))) AS records_per_second
# MAGIC
# MAGIC FROM 
# MAGIC   cte2
# MAGIC GROUP BY 
# MAGIC   endTime
# MAGIC

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Photon 5 files
# MAGIC %sql
# MAGIC with cte as (
# MAGIC select
# MAGIC to_timestamp(kafka_silver_datetime) as endTime 
# MAGIC , to_timestamp(kafkaWritetime) as startTime
# MAGIC , unix_timestamp(to_timestamp(kafka_silver_datetime)) - unix_timestamp(to_timestamp(kafkaWritetime)) as diffSeconds
# MAGIC
# MAGIC from kafka_silver
# MAGIC )
# MAGIC
# MAGIC select 
# MAGIC endTime, 
# MAGIC startTime, 
# MAGIC diffSeconds, 
# MAGIC count(1) as recordCount,
# MAGIC count(1)/diffSeconds as rec_per_second
# MAGIC
# MAGIC from cte 
# MAGIC group by endTime, startTime, diffSeconds
# MAGIC order by startTime desc
# MAGIC

# COMMAND ----------

# DBTITLE 1,Photon 1 file
# MAGIC %sql
# MAGIC with cte as (
# MAGIC select
# MAGIC to_timestamp(kafka_silver_datetime) as endTime 
# MAGIC , to_timestamp(kafkaWritetime) as startTime
# MAGIC , unix_timestamp(to_timestamp(kafka_silver_datetime)) - unix_timestamp(to_timestamp(kafkaWritetime)) as diffSeconds
# MAGIC
# MAGIC from kafka_silver
# MAGIC )
# MAGIC
# MAGIC select 
# MAGIC endTime, 
# MAGIC startTime, 
# MAGIC diffSeconds, 
# MAGIC count(1) as recordCount,
# MAGIC count(1)/diffSeconds as rec_per_second
# MAGIC
# MAGIC from cte 
# MAGIC group by endTime, startTime, diffSeconds
# MAGIC order by startTime desc
# MAGIC

# COMMAND ----------

# DBTITLE 1,Photon - 1 file - 16k records a second
# MAGIC %sql
# MAGIC
# MAGIC with cte as (
# MAGIC select
# MAGIC to_timestamp(kafka_silver_datetime) as endTime 
# MAGIC , to_timestamp(kafkaWritetime) as startTime
# MAGIC , unix_timestamp(to_timestamp(kafka_silver_datetime)) - unix_timestamp(to_timestamp(kafkaWritetime)) as diffSeconds
# MAGIC
# MAGIC from kafka_silver
# MAGIC ), cte2 as (
# MAGIC select 
# MAGIC endTime, 
# MAGIC startTime, 
# MAGIC diffSeconds, 
# MAGIC count(1) as recordCount,
# MAGIC count(1)/diffSeconds as rec_per_second
# MAGIC
# MAGIC from cte 
# MAGIC group by endTime, startTime, diffSeconds
# MAGIC order by startTime desc
# MAGIC ) 
# MAGIC
# MAGIC
# MAGIC SELECT 
# MAGIC   endTime, 
# MAGIC   SUM(recordCount) AS total_records,
# MAGIC   UNIX_TIMESTAMP(MAX(endTime)) - UNIX_TIMESTAMP(MIN(startTime)) AS total_seconds,
# MAGIC   SUM(recordCount) / (UNIX_TIMESTAMP(MAX(endTime)) - UNIX_TIMESTAMP(MIN(startTime))) AS records_per_second
# MAGIC
# MAGIC FROM 
# MAGIC   cte2
# MAGIC GROUP BY 
# MAGIC   endTime
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from kafka_silver

# COMMAND ----------



# COMMAND ----------


