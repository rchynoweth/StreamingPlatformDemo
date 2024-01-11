from pyspark.sql.functions import *

class JDBCConnect():

  def __init__(self, jdbcUsername, jdbcPassword, jdbcUrl):
    """
    :param jdbcUsername: username to authenticate against JDBC endpoint
    :param jdbcPassword: password to authenticate against JDBC endpoint
    :param jdbcUrl: JDBC URL to authenticate against
    """
    self.jdbcUsername = jdbcUsername
    self.jdbcPassword = jdbcPassword
    self.jdbcUrl = jdbcUrl

  def read_jdbc_table(self, spark, jdbc_table_name):
    """
    Reads a given JDBC table and returns a Spark DataFrame

    :param spark: Spark Object
    :param jdbc_table_name: the name of the table to read from the jdbc database
    :returns: Spark Dataframe representing the DLT table
    """
    return (spark.read
            .format('jdbc')
            .option("url", self.jdbcUrl)
            .option("dbtable", jdbc_table_name)
            .option("user", self.jdbcUsername)
            .option("password", self.jdbcPassword)
            .load()
            )
  
  def read_jdbc_query(self, spark, sql_query):
    """
    Reads a given JDBC table and returns a Spark DataFrame

    :param spark: Spark Object
    :param jdbc_table_name: the name of the table to read from the jdbc database
    :returns: Spark Dataframe representing the DLT table
    """
    return (spark.read
            .format('jdbc')
            .option("url", self.jdbcUrl)
            .option("query", sql_query)
            .option("user", self.jdbcUsername)
            .option("password", self.jdbcPassword)
            .load()
            )
    
  def batch_write_jdbc_table(self, df, jdbc_table_name, save_mode='append'):
    """
    Writes a given Spark DataFrame to a JDBC table

    :param df: Spark DataFrame to write
    :param jdbc_table_name: name of the target jdbc table 
    :param save_mode: default is append
    """
    (
      df.write
      .format('jdbc')
      .option("url", self.jdbcUrl)
      .option("dbtable", jdbc_table_name)
      .option("user", self.jdbcUsername)
      .option("password", self.jdbcPassword)
      .mode(save_mode)
      .save()
    )


  def foreach_write_stream(self, microBatchDF, batchId, jdbc_table_name, save_mode='append'):
    """
    Foreach batch function to write to a JDBC sink as a stream
    """
    (microBatchDF.write
      .format('jdbc')
      .option("url", self.jdbcUrl)
      .option("dbtable", jdbc_table_name)
      .option("user", self.jdbcUsername)
      .option("password", self.jdbcPassword)
      .mode(save_mode)
      .save() 
    )


  def stream_write_jdbc_table(self, df, jdbc_table_name, available_now=None, processing_time="0 seconds", checkpoint_location=None,, save_mode='append'):
    """
    Writes a given Spark Streaming DataFrame to a JDBC table using a foreach batch function 

    :param df: Spark Streaming DataFrame to write
    :param jdbc_table_name: name of the target jdbc table 
    :param checkpoint_location: stream checkpoint location i.e. "abfss://..."
    :param save_mode: default is append
    """
    assert available_now == None or processing_time == None
    assert checkpoint_location is not None

    (df.writeStream
      .option("checkpointLocation", checkpoint_location)
      .trigger(availableNow=available_now, processingTime=processing_time)
      .foreachBatch(lambda df,epochId: self.foreach_write_stream(df, epochId, jdbc_table_name, save_mode)) 
      .start()
    )
