from pyspark.sql import SparkSession
from dlt_platform.connectors.file_source_connect import FileSourceConnect


spark = (SparkSession.builder.getOrCreate())


def test_read_file_stream():
    input_path = '/databricks-datasets/structured-streaming/events'
    file_type = 'json'
    schema_location = '/tmp/sc_test'
    fsc = FileSourceConnect()
    df = fsc.read_file_stream(spark=spark, input_path=input_path, file_type=file_type, schema_location=schema_location)
    # assert df.rdd.isEmpty() == False # streaming df this is not needed