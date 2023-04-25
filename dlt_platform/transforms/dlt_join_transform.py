import dlt
from pyspark.sql.functions import * 

class DLTJoinTransform():
  """
  Class used for joining streams in a DLT pipeline. 
  
  Please use the normal pyspark join function for non-DLT workloads. 
  """

  def stream_static_join(self, stream1, stream2, join_keys, join_type='inner'):
    """ 
    Given two stream names it will return the result set of the join. 

    :param stream1: name of the first stream. This is the streaming dataset. 
    :param stream2: name of the second stream. This is the static dataset. 
    :param on: the columns to join on. Can be string, list, or columns. 
    :param join_type: type of join to execute. Please see pyspark.sql.DataFrame.join for more details. 
    """
    stream_df = dlt.read_stream(stream1)
    static_df = dlt.read(stream2)
    return (stream_df.join(static_df, join_keys, join_type))

  def static_static_join(self, stream1, stream2, join_keys, join_type='inner'):
    """ 
    Given two stream names it will return the result set of the join. 

    :param stream1: name of the first stream. This is a batch read on the dataset. 
    :param stream2: name of the second stream. This is a batch read on the dataset. 
    :param on: the columns to join on. Can be string, list, or columns. 
    :param join_type: type of join to execute. Please see pyspark.sql.DataFrame.join for more details. 
    """
    static_df1 = dlt.read(stream1)
    static_df2 = dlt.read(stream2)
    return (static_df1.join(static_df2, join_keys, join_type))
