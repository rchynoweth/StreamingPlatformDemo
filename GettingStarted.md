# Getting Started 

`nrtd` is a Python package that provides connectors and transformations to build streaming data pipelines. The `nrtd` library can be used with two products in Databricks: 
1. Spark Structured Streaming running on Job Clusters  
1. Delta Live Tables (DLT) pipeline

The NRTD recommends that all pipelines default to the Delta Live Table API unless specific requirements need Spark Structured Streaming. Robust Streaming ETL solutions are much more managable and cost effective on DLT and allow users to change between continuous streaming and batch processing with zero code changes allowing us to "right-time" pipelines to optimize our return on investment. 

Delta Live Tables simplifies the streaming process and enables the team to maximize compute resources while providing query scheduling, cluster load balancing, enhanced autoscaling, data quality checks, and more. Delta Live Tables is purposed for streaming ETL or streaming use cases with forgivable latency requirements. Delta Live Tables is more cost effective than structured streaming as well due to its ability to maximize compute resources.  

Spark Structured Streaming should be used for more simple use cases and when in-memory processing and low latency is extremely important. If you are familiar with and have experience with Spark Structured Streaming then also feel free to use it over Delta Live Tables. 

For example, if we are loading many tables and performing streaming aggregates then we should use DLT. If you are looking to send an alert and join to a static lookup table with a low SLA then you should use Spark Structured Streaming. 

Please work with the NRTD team to determine which API is best for you. 


## Templates 

We have developed a number of templates that can be reused or referenced to meet specific technical requirements. Please see below for available templates:
- [Template 1](https://google.com)
- [Template 2](https://google.com)
- [Template 3](https://google.com)


## Installation  

The best way to install the `nrtd` library is by placing the installation at the entrypoint of the job. To install the library via a notebook please insert the following in the **first** cell: 

ZACK TO CHANGE THE URL - this can be directly from git repo or from a wheel file in artifactory/storage
```
%pip install git+https://github.com/rchynoweth/StreamingTemplates.git@main
```


## Examples 

ZACK TO CHANGE CODE EXAMPLES AS NEEDED. 


The `nrtd` library supports both Delta Live Tables and Spark Structured Streaming. 

### Spark Structured Streaming 

[Spark Structured Streaming](https://docs.databricks.com/structured-streaming/index.html) is the most popular streaming engine for real-time analytics and operational use cases. 

```python
# Custom NRTD Python Library - i.e. "templates"
from nrtd.connectors.kafka_connect import KafkaConnect
from nrtd.connectors.file_source_connect import FileSourceConnect

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

# Custom NRTD Python Library - i.e. "templates"
from nrtd.connectors.kafka_connect import KafkaConnect
from nrtd.connectors.file_source_connect import FileSourceConnect

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
