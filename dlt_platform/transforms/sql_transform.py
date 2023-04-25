

class SQLTransform():
  """
  Class used for custom SQL transformations  
  """

  def spark_sql(self, spark, sql):
    """ 
    Given a SQL string it will return a the results as a DataFrame 

    :param spark: Spark Contect
    :param sql: SQL string
    """
    return ( spark.sql(sql) )

