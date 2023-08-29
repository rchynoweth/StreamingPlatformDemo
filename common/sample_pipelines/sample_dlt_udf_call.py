# Databricks notebook source
# MAGIC %pip install git+https://github.com/rchynoweth/StreamingPlatformDemo.git@main

# COMMAND ----------

# MAGIC %md
# MAGIC UDFs require us to install the package as a wheel file. Which is fine because that is how we will deploy in production. 

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dlt_platform.transforms.call_api_transform import UDFTransform
from dlt_platform.connectors.jdbc_connect import JDBCConnect

# COMMAND ----------

udft = UDFTransform()

# COMMAND ----------

@dlt.table(name="bronze_postman_table")
def bronze_postman_table():
  columns = ["language","users_count"]
  data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
  rdd = spark.sparkContext.parallelize(data)
  df = rdd.toDF(columns)
  return df

# COMMAND ----------

@dlt.table(name="silver_postman_table")
def silver_postman_table():
  dog_url = "https://dog.ceo/api/breeds/list/all"
  df = (dlt.read('bronze_postman_table')
        .withColumn("cat_col", udft.api_udf(col("language"))) 
        .withColumn("dog_col", udft.api_udf(col("language"), lit(dog_url)))
       )
  return df

# COMMAND ----------

@dlt.table(name="gold_postman_table")
def gold_postman_table():
  cat_schema = ArrayType(StructType([
    StructField("__v", StringType()),
    StructField("_id", StringType()),
    StructField("createdAt", StringType()),
    StructField("deleted", StringType()),
    StructField("source", StringType()),
    StructField("status", StructType([
      StructField("feedback", StringType()),
      StructField("sentCount", StringType()),
      StructField("verified", StringType())
    ]) ),
    StructField("text", StringType()),
    StructField("type", StringType()),
    StructField("updatedAt", StringType()),
    StructField("used", StringType()),
    StructField("user", StringType())
    ])
   )
  
  df = (dlt.read("silver_postman_table")
        .withColumn('exploded_cat_col' , explode(from_json('cat_col', cat_schema)))
       )
  return df 

# COMMAND ----------


