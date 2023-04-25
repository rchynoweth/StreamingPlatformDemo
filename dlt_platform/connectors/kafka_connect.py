from pyspark.sql.functions import *
from pyspark.sql.types import * 

# Source: https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/kafka 
## Likely add authentication as needed: https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/kafka#--configure-kafka-for-apache-spark-on-azure-databricks 

class KafkaConnect():

  def __init__(self, kafka_bootstrap_servers):
    """
    :param kafka_bootstrap_servers: The Kafka bootstrap servers to connect to
    """
    self.kafka_bootstrap_servers = kafka_bootstrap_servers
  
  
  def read_kafka_stream(self, spark, topic, username, security_protocol, sasl_mechanism, starting_offsets = "earliest", fail_on_data_loss="false"):
    """
    Reads a given Kafka topic and returns a streaming dataframe. 
    This function requires the exact options needed to read. 
    
    Source: https://spark.apache.org/docs/2.1.1/structured-streaming-kafka-integration.html

    :param spark: Spark Object
    :param topic: the topic to read
    :param username: username to use for authentication
    :param security_protocol: encryption protocol 
    :param sasl_mechanism: authentication mechanism 
    :param starting_offsets: starting offset value. Default 'earliest'. 
    :param fail_on_data_loss: Whether to fail the query when it's possible that data is lost (e.g., topics are deleted, or offsets are out of range).
    :returns: Spark Streaming Dataframe representing the DLT table
    """
    return (spark.readStream
            .format("kafka")
            .option("kafka.sasl.jaas.config", f"kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username='{username}';")
            .option("kafka.security.protocol", security_protocol)
            .option("kafka.sasl.mechanism", sasl_mechanism)
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) 
            .option("subscribe", topic )
            .option("startingOffsets", starting_offsets )
            .option("failOnDataLoss", fail_on_data_loss)
            .load()
           )

  def generic_read_kafka_stream(self, spark, options):
    """
    Reads a given Kafka topic and returns a streaming dataframe. 
    This function allows users to pass any options they wish. 

    :param spark: Spark Object
    :param options: the options to read from the kafka topic
    :returns: Spark Streaming Dataframe representing the DLT table
    """
    assert type(options) == dict
    return (spark.readStream
            .format("kafka")
            .options(**options)
            .load()
           )  

  def write_kafka_stream_plaintext(self, df, key_col, value_cols, topic, checkpoint_location):
    """
    Writes a given Spark Streaming Dataframe to a Kafka topic

    :param df: Spark Streaming Dataframe to write to Kafka
    :param key_col: the column to represent the key value in Kafka
    :param value_cols: list of column names (e.g. ['col1', 'col2']) to send as the value
    :param topic: name of topic to write to
    :param checkpoint_location: location of streaming checkpoint
    """
    (df
     .select(col(key_col).alias("key"), to_json(struct([x for x in value_cols])).alias("value"))
     .writeStream
     .format("kafka")
     .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
     .option("kafka.security.protocol", "PLAINTEXT")
     .option("checkpointLocation", checkpoint_location )
     .option("topic", topic)
     .start()
    )
    
    
  def write_kafka_stream(self, df, key_col, value_cols, topic, checkpoint_location, username, security_protocol, sasl_mechanism):
    """
    Writes a given Spark Streaming Dataframe to a Kafka topic

    :param df: Spark Streaming Dataframe to write to Kafka
    :param key_col: the column to represent the key value in Kafka
    :param value_cols: list of column names (e.g. ['col1', 'col2']) to send as the value
    :param topic: name of topic to write to
    :param checkpoint_location: location of streaming checkpoint
    :param username: username to use for authentication
    :param security_protocol: encryption protocol 
    :param sasl_mechanism: authentication mechanism 
    """
    (df
     .select(col(key_col).alias("key"), to_json(struct([x for x in value_cols])).alias("value"))
     .writeStream
     .format("kafka")
     .option("kafka.sasl.jaas.config", f"kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username='{username}';")
     .option("kafka.security.protocol", security_protocol)
     .option("kafka.sasl.mechanism", sasl_mechanism)
     .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) 
     .option("checkpointLocation", checkpoint_location )
     .option("topic", topic)
     .start()
    )

    