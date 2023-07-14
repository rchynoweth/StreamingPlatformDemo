# StreamingTemplates

This repository is an example of how to build a Python library to further abstract data processing, ETL, and Streaming on Databricks. The core product being used is Delta Live Tables, however, much of the code (writing to Kafka) works running as traditional Databricks Cluster compute. Please use this repository as an example of the ability to modularize code when working in DLT. 

While DLT is a simplified streaming platform, there is often a desire to further modularize code to make it more simple for other users to connect to data sources and sinks. For example, an organization can set a standard way to read/write to Kafka topics that allows users to do so with a single line of code and share required environment variables. Or we can organize common transformations so that we can reuse code across pipelines to reduce technical overhead. 

## Installation and Requirements 

- Databricks Runtime (Interactive and Jobs Compute)
- Databricks - Delta Live Tables (Technically optional but recommended - some modules may only work on DLT)

For the purposes of this demo we will simply allow users to install from source with the following command inside the first cell of a Databricks notebook. Please note that you can also install this repo by cloning it to the workspace and attaching it to a cluster:
```
# in a notebook 
%pip git+https://github.com/rchynoweth/StreamingTemplates.git@main
```

If you wish to package this repo as a wheel file, please clone the code then run the following command: 
```
python setup.py bdist_wheel   
```

To generate documentation for the repo we are using `pdoc` and you can use the following command from the base directory. 
```
pdoc -o docs dlt_platform
```

## Getting Started 

`dlt_platform` is a Python package that provides connectors and transformations to build streaming data pipelines. The `dlt_platform` library can be used with two products in Databricks: 
1. Spark Structured Streaming running on Job Clusters  
1. Delta Live Tables (DLT) pipeline

I recommend that all pipelines by default use the Delta Live Table API unless specific requirements need Spark Structured Streaming. Robust Streaming ETL solutions are much more managable and cost effective on DLT and allow users to change between continuous streaming and batch processing with zero code changes allowing us to "right-time" pipelines to optimize our return on investment. 

## Templates 

We have developed a number of templates that can be reused or referenced to meet specific technical requirements. Please see below for available templates:
- [JDBC Connect](https://github.com/rchynoweth/StreamingPlatformDemo/blob/main/common/sample_pipelines/sample_streaming_jdbc.py)
- [UDF Transform](https://github.com/rchynoweth/StreamingPlatformDemo/blob/main/common/sample_pipelines/sample_dlt_udf_call.py)
- [Sample Pipeline](https://github.com/rchynoweth/StreamingPlatformDemo/blob/main/common/sample_pipelines/sample_dlt_pipeline.py)



### Examples 

The `dlt_platform` library supports both Delta Live Tables and Spark Structured Streaming. 

#### Spark Structured Streaming 

[Spark Structured Streaming](https://docs.databricks.com/structured-streaming/index.html) is the most popular streaming engine for real-time analytics and operational use cases. 

```python
# Custom  Python Library - i.e. "templates"
from dlt_platform.connectors.kafka_connect import KafkaConnect
from dlt_platform.connectors.file_source_connect import FileSourceConnect

k = KafkaConnect(kafka_servers)

# read kafka topic
df = k.read_kafka_stream(spark=spark,topic=topic) 

# write kafka topic 
write_kafka_stream(df, key_col, value_cols, topic, checkpoint_location, username, security_protocol, sasl_mechanism)

```


### Delta Live Tables 

[Delta Live Tables](https://docs.databricks.com/delta-live-tables/index.html) simplifies streaming pipeline development and deployment by allowing Databricks to manage many of the complexity that the user qas required to handle. It introduces a slightly different syntax 
```python
import dlt 

# Custom Python Library - i.e. "templates"
from dlt_platform.connectors.kafka_connect import KafkaConnect
from dlt_platform.connectors.file_source_connect import FileSourceConnect

k = KafkaConnect(kafka_servers)
f = FileSourceConnect()

# read kafka topic
@dlt.table(name='kafka_events')
def kafka_events():
  return ( k.read_kafka_stream(spark=spark,topic=topic) )

# stream read files from directory 
@dlt.table(name='test_file_events')
def test_file_events():
  return f.read_file_stream(spark, '/databricks-datasets/structured-streaming/events', 'json')

```
