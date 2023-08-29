# Databricks notebook source
# MAGIC %pip install git+https://github.com/rchynoweth/StreamingPlatformDemo.git@main

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from dlt_platform.transforms.call_api_transform import UDFTransform

# COMMAND ----------

udft = UDFTransform()

# COMMAND ----------

columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
rdd = spark.sparkContext.parallelize(data)
df = rdd.toDF(columns)
display(df)

# COMMAND ----------

df = df.withColumn("new_col", udft.api_udf(col("language")))
display(df)

# COMMAND ----------

js_sc = ArrayType(StructType([
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

# COMMAND ----------

display(df.withColumn('exploded_col' , explode(from_json('new_col', js_sc)))
        .select(['exploded_col', 'exploded_col.createdAt', 'exploded_col.__v', 'exploded_col._id', 'exploded_col.deleted'])

                )

# COMMAND ----------

dog_url = "https://dog.ceo/api/breeds/list/all"
df = df.withColumn("new_col2", udft.api_udf(col("language"), lit(dog_url)))
display(df)

# COMMAND ----------


