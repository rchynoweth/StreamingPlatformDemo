
class DeltaLakeConnect():

  
  def read_stream_delta_table(self, spark, table_name):
    """
    Reads a given table as a stream

    :param spark: Spark Object
    :param table_name: the name of the table to be returned to DLT. 
    :returns: Spark Streaming Dataframe representing the DLT table
    """
    return (spark.readStream
        .format("delta")
        .table(table_name)
      )

  def write_stream_delta_table(self, df, table_name, checkpoint_location):
    """ Writes a given Spark DataFrame to a table as a stream """
    return (df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_location)
        .toTable(table_name)
      )
  

  def read_stream_delta_path(self, spark, path):
    """
    Reads a given table path as a stream

    :param spark: Spark Object
    :param table_name: the name of the table to be returned to DLT. 
    :returns: Spark Streaming Dataframe representing the DLT table
    """
    return (spark.readStream
        .format("delta")
        .load(path)
      )

  def write_stream_delta_path(self, df, path, checkpoint_location):
    """ Writes a given Spark DataFrame to a path location as a stream """
    return (df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_location)
        .start(path)
      )
