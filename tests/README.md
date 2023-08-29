%md
# Testing 


In our example we are publishing data to a kafka topic with a Databricks job. We then read that data as a stream into DLT where we have an ingestion table and a streaming target table parsing out the data definition. Please note that the number of rows for each batch varies slightly as we restrict the number of files read and each file is approximately the same size but may vary slightly.  

We have observed the following metrics:
- Reading from Kafka and processing through 2 tables we see about 60k records per second **without** photon 
- Reading from Kafka and processing through 2 tables we see about __k records per second **with** photon 