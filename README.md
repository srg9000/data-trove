# data-trove

## Setup
1. Setup environment (Using poetry)
```sh
poetry shell
poetry install
```

2. Setup kafka (Local or docker)
```
sudo docker pull apache/kafka
sudo docker run -p 9092:9092 apache/kafka
```

3. Kafka topic setup and publishing utility class is available in `kafka_connector.py`
4. Utility class for streaming data with pyspark from kafka topics is available in `spark_connector.py`



## Data Ingestion:
Apache Kafka is used for combining data from ad impressions (JSON), clicks/conversions (CSV), and bid requests (Avro) data, with different methods for each data type, publishing data in given topic.

## Data Processing:
Data processing is done using pyspark. The pyspark connector class `SparkConnector` implements methods to read streaming data from kafka topics, merge them and run user defined sql queries.

## Data Storage and Query Performance:
The `SparkConnector` class has methods to store streaming data in parquet files as well as storing batches of data in apache hbase which offers optimized storage and fast query performance.

## Error Handling and Monitoring:
Methods like `check_data_quality` in the spark connector can be used to incorporate domain specific quality checks and can be overridden as well