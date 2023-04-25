from pyspark.sql.functions import *

class EDWConnect():

  def __init__(self, username, password, url, warehouse_name):
    """
    :param username: username to authenticate 
    :param password: password to authenticate 
    :param url: URL to authenticate against
    """
    self.username = username
    self.password = password
    self.url = url
    self.warehouse_name = warehouse_name

  def read_table(self, spark, database_name, schema_name, table_name):
    """
    Reads a given table and returns a Spark DataFrame

    :param spark: Spark Object
    :param database_name: database of table object
    :param schema_name: schema of table object
    :param table_name: the name of the table to read from the database
    :returns: Spark Dataframe representing the DLT table
    """
    return (spark.read
            .format("snowflake")
            .option("dbtable", table_name)
            .option("sfUrl", self.database_host_url)
            .option("sfUser", self.username)
            .option("sfPassword", self.password)
            .option("sfDatabase", database_name)
            .option("sfSchema", schema_name)
            .option("sfWarehouse", self.warehouse_name)
            .load()
          )
    
  def batch_write_table(self, df, database_name, schema_name, table_name, save_mode='append'):
    """
    Writes a given Spark DataFrame to a table

    :param df: Spark DataFrame to write
    :param database_name: database of table object
    :param schema_name: schema of table object
    :param table_name: the name of the table to read from the database
    :param save_mode: default is append
    """
    (
      df.write
      .format('snowflake')
      .option("sfUrl", self.database_host_url)
      .option("sfUser", self.username)
      .option("sfPassword", self.password)
      .option("sfDatabase", database_name)
      .option("sfSchema", schema_name)
      .option("sfWarehouse", self.warehouse_name)
      .option("dbtable", table_name)
      .mode(save_mode)
      .save()
    )


  def foreach_write_stream(self, microBatchDF, batchId, database_name, schema_name, table_name, save_mode='append'):
    """
    Foreach batch function to write to a SF sink as a stream
    """
    (microBatchDF.write
      .format('snowflake')
      .option("sfUrl", self.database_host_url)
      .option("sfUser", self.username)
      .option("sfPassword", self.password)
      .option("sfDatabase", database_name)
      .option("sfSchema", schema_name)
      .option("sfWarehouse", self.warehouse_name)
      .option("dbtable", table_name)
      .mode(save_mode)
      .save()
    )


  def stream_write_table(self, df, database_name, schema_name, table_name, checkpoint_location, save_mode='append'):
    """
    Writes a given Spark Streaming DataFrame to a table using a foreach batch function 

    :param df: Spark Streaming DataFrame to write
    :param database_name: database of table object
    :param schema_name: schema of table object
    :param table_name: the name of the table to read from the database
    :param checkpoint_location: stream checkpoint location i.e. "abfss://..."
    :param save_mode: default is append
    """
    (df.writeStream
    .option("checkpointLocation", checkpoint_location)
    # .trigger(Once=True) # parameterize this to allow different trigger types?
    .foreachBatch(lambda df,epochId: self.foreach_write_stream(df, epochId, database_name, schema_name, table_name, save_mode)) 
    .start()
    )
