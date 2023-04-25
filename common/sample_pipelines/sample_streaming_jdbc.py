# Databricks notebook source
from dlt_platform.connectors.jdbc_connect import JDBCConnect

# COMMAND ----------

jdbcUsername = ''
jdbcPassword = ''
jdbcHostname = ""
jdbcPort = 1433
jdbcDatabase = ''


tableName = "dbo.rac_table"

# Create the JDBC URL without passing in the user and password parameters.
jdbcUrl = "jdbc:sqlserver://{}:{};database={}".format(
  jdbcHostname, jdbcPort, jdbcDatabase
)

# COMMAND ----------

j_conn = JDBCConnect(jdbcUsername=jdbcUsername, jdbcPassword=jdbcPassword, jdbcUrl=jdbcUrl)

# COMMAND ----------

# DBTITLE 1,Batch Read
df = j_conn.read_jdbc_table(spark, 'sys.tables')
display(df)

# COMMAND ----------

# DBTITLE 1,Batch Write
j_conn.batch_write_jdbc_table(df, 'dbo.rac_sys_tables')
df = j_conn.read_jdbc_table(spark, 'dbo.rac_sys_tables')
display(df)

# COMMAND ----------

# DBTITLE 1,Streaming Write to Delta
df.write.mode('overwrite').saveAsTable('rac_delta_stream_source')

# COMMAND ----------

# DBTITLE 1,Streaming Read from Delta (need a stream df for testing)
stream_df = spark.readStream.option("ignoreChanges", "true").table('rac_delta_stream_source')

# COMMAND ----------

# DBTITLE 1,Stream Write to JDBC
j_conn.stream_write_jdbc_table(df=stream_df, jdbc_table_name='dbo.rac_stream_sink', checkpoint_location='/dbfs/temp/ryanchynoweth4')

# COMMAND ----------


