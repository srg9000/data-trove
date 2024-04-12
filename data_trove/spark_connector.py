import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'


class SparkConnector:
    def __init__(self, app_name="KafkaDataMerge", kafka_bootstrap_server="localhost:9092"):
        self.kafka_bootstrap_server = "localhost:9092"
        self.topic_dfs = {}
        # Create a SparkSession
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()

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

    def merge_dfs(self):
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

    def write_to_hbase(self, df, table_name, column_family):
        # Define HBase configuration
        conf = {
            "hbase.zookeeper.quorum": "localhost",  # HBase ZooKeeper quorum
            "hbase.zookeeper.property.clientPort": "2181",  # ZooKeeper client port
            "hbase.client.scanner.timeout.period": "100000"  # Scanner timeout
        }
        # Write DataFrame to HBase
        df.writeStream \
            .trigger(processingTime='5 seconds') \
            .format("parquet") \
            .option("checkpointLocation", "./parquet/files") \
            .start("./parquet/") \
            .awaitTermination()

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
    connector.write_to_hbase(merged_df, "example_table", "cf")
    connector.stop_session()