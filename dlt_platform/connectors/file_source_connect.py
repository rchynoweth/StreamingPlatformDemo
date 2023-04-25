
class FileSourceConnect():

  
  def read_file_stream(self, spark, input_path, file_type, schema_location=None):
    """
    Reads a given directory as a stream

    :param spark: Spark Object
    :param table_name: the name of the table to be returned to DLT
    :param input_path: the directory to read
    :param file_type: the type of file to read in the directory 
    :param schema_location: schema location is required if working outside of DLT. 
    :returns: Spark Streaming Dataframe representing the DLT table
    """
    if schema_location is None:
      return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_type)
        .load(input_path)
      )
    else :
      return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_type)
        .option("cloudFiles.schemaLocation", schema_location)
        .load(input_path)
      )
  
  def write_file_stream(self, streamDF, output_path, file_type='json', write_mode='append'):
    """
    Writes a given streaming dataframe to an output location. 
    """
    (streamDF
      .write
      .format(file_type)
      .mode(write_mode)
      .save(output_path)
    )