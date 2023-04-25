# StreamingTemplates

This repository is an example of how to build a Python library to further abstract data processing, ETL, and Streaming on Databricks. The core product being used is Delta Live Tables, however, much of the code (writing to Kafka) works running as traditional Databricks Cluster compute. Please use this repository as an example of the ability to modularize code when working in DLT. 

While DLT is a simplified streaming platform, there is often a desire to further modularize code to make it more simple for other users to connect to data sources and sinks. For example, an organization can set a standard way to read/write to Kafka topics that allows users to do so with a single line of code and share required environment variables. Or we can organize common transformations so that we can reuse code across pipelines to reduce technical overhead. 

## Installation and Requirements 

- Databricks Runtime (Interactive and Jobs Compute)
- Databricks - Delta Live Tables (Technically optional but recommended - some modules may only work on DLT)

For the purposes of this demo we will simply allow users to install from source with the following command inside a Databricks notebook. Please note that you can also install this repo by cloning it to the workspace and attaching it to a cluster:
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