import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging
logger = logging.getLogger(__name__)
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'


class SparkConnector:
    def __init__(self, app_name="KafkaDataMerge",
            kafka_bootstrap_server="localhost:9092", expected_fields=[]):
        self.kafka_bootstrap_server = "localhost:9092"
        self.topic_dfs = {}
        # Create a SparkSession
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()
        self.expected_fields = expected_fields

    def subscribe_kafka_topics(self, topic_name):
        # Read data from Kafka topics
        topic_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_server) \
            .option("subscribe", topic_name) \
            .load()
        topic_df.printSchema()
        self.topic_dfs[topic_name] = topic_df

    def data_processing(self, df):
        # Apply data transformation, validation, filtering, deduplication, etc.
        processed_df = df  # Placeholder, implement your processing logic here
        processed_df = processed_df.filter(col("value").isNotNull())  # Example filter
        # Add more processing logic as needed
        return processed_df

    def merge_dfs(self):
        # Merge dataframes to combine information from different sources
        df_values = list(self.topic_dfs.values())
        if len(df_values) == 0:
            return
        merged_df = df_values[0]
        for i in df_values[1:]:
            merged_df = merged_df.union(i)
        return merged_df

    def run_query(self, df):
        # Define query to output merged data
        query = df \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .start()

        # Start the query
        query.awaitTermination()
        return query

    def execute_sql_query(self, sql_query, df):
        # Execute Spark SQL query on DataFrame
        result_df = df.spark.sql(sql_query)
        return result_df

    def write_to_parquet(self, df, table_name, column_family, timeout=10):
        self.check_data_quality(df)
        # Write DataFrame stream to parquet
        df.writeStream \
            .format("parquet") \
            .option("checkpointLocation", "./parquet/checkpoints") \
            .start("./parquet/files") \
            .awaitTermination(timeout)

    def write_to_hbase(self, df, table_name, column_family):
        # Write DataFrame stream to HBase
        def write_to_hbase_batch(df, epoch_id):
            # Define HBase configuration
            hbase_config = {
                "hbase.zookeeper.quorum": os.getenv("ZOOKEEPER_QUORUN"),  # HBase ZooKeeper quorum
                "hbase.zookeeper.property.clientPort": os.getenv("ZOOKEEPER_PORT"),  # ZooKeeper client port
                "hbase.client.scanner.timeout.period": os.getenv("SCANNED_TIMEOUT")  # Scanner timeout
            }

            # Write DataFrame to HBase
            df.write \
                .options(catalog=f'hbase://{table_name}/{column_family}', newTable="5") \
                .format("org.apache.hadoop.hbase.spark") \
                .save()

        # Write streaming DataFrame to HBase using foreachBatch
        df.writeStream \
            .foreachBatch(write_to_hbase_batch) \
            .start() \
            .awaitTermination()

    def check_data_quality(self, df):
        # Completeness check: Verify if all expected fields are present
        expected_fields = self.expected_fields  # Define expected fields
        missing_fields = set(expected_fields) - set(df.columns)
        if missing_fields:
            logger.error(f"QUALITY ERROR: Missing fields: {missing_fields}")

    def stop_session(self):
        # Stop the SparkSession
        self.spark.stop()


if __name__ == '__main__':
    # Define Kafka topics
    impressions_topic = "ad_impressions"
    clicks_conversions_topic = "clicks_conversions"
    bid_requests_topic = "bid_requests"
    topic_names = [
        impressions_topic,
        clicks_conversions_topic,
        bid_requests_topic
    ]
    connector = SparkConnector()
    for topic in topic_names:
        connector.subscribe_kafka_topics(topic)

    # Process data (if needed)
    # You can apply transformations, filtering, joins, etc., as required
    # For example:
    # impressions_df_processed = impressions_df.select(col("value").cast("string").alias("impressions_data"))

    # Merge dataframes
    merged_df = connector.merge_dfs()
    connector.write_to_parquet(merged_df, "example_table", "cf")
    # connector.write_to_hbase(merged_df, "example_table", "cf")
    connector.stop_session()