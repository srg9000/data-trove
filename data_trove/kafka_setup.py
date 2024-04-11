from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer, KafkaConsumer
import json
import csv
import datetime
from avro import schema, datafile, io


class KafkaClient:
    def __init__(self, bootstrap_servers=None):
        self.bootstrap_servers = bootstrap_servers
        if self.bootstrap_servers is None:
            self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVER', 'localhost:9092')
        self.admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
            
    def create_topics(self, topic_name, num_partitions=1, replication_factor=1):
        # Create Kafka topics
        if topic_name in self.list_topics():
            print("TOPIC ALREADY EXISTS")
            return False
        topics = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
        self.admin_client.create_topics(new_topics=topics, validate_only=False)
        return True

    def list_topics(self):
        return self.admin_client.list_topics()

    def publish_json(self, topic_name, json_messages):
        producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        for message in ad_impressions:
            producer.send(topic_name, value=message)
        producer.flush()
        producer.close()

    # Publish messages to Kafka topics
    def publish_messages(self):
        # Example bid requests data
        bid_schema = schema.parse(open("bid_request.avsc", "rb").read())
        writer = datafile.DataFileWriter(open("bid_requests.avro", "wb"), io.DatumWriter(), bid_schema)
        writer.append({"user_id": "user789", "auction_id": 123, "ad_targeting_criteria": ["male", "25-34"]})
        writer.close()
        
        # Publish messages to topics
        
        for message in clicks_conversions:
            producer.send(clicks_conversions_topic, value=message)
        
        # Publish Avro messages to topic
        with open("bid_requests.avro", "rb") as f:
            producer.send(bid_requests_topic, value=f.read())

        producer.flush()
        producer.close()

    # Subscribe to Kafka topics
    def subscribe_to_topics(self, topic_list):
        consumer = KafkaConsumer(bootstrap_servers=self.bootstrap_servers,
                                 auto_offset_reset='earliest',
                                 value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        
        consumer.subscribe(topics=topic_list)
        
        for message in consumer:
            print(f"Received message: {message.value} from topic: {message.topic}")

if __name__ == "__main__":
    # Kafka broker properties
    bootstrap_servers = 'localhost:9092'

    # Topics
    impressions_topic = 'ad_impressions'
    clicks_conversions_topic = 'clicks_conversions'
    bid_requests_topic = 'bid_requests'
    # Create client
    client = KafkaClient(bootstrap_servers)
    # Create topics
    client.create_topics(impressions_topic, 1, 1)
    client.create_topics(clicks_conversions_topic, 1, 1)
    client.create_topics(bid_requests_topic, 1, 1)
    # List topics
    topics = client.list_topics()
    print(topics)
    # Example ad impressions data
    ad_impressions = [
        {"ad_creative_id": 1, "user_id": "user123", "timestamp": "2024-04-06T12:00:00", "website": "example.com"},
        {"ad_creative_id": 2, "user_id": "user456", "timestamp": "2024-04-06T12:01:00", "website": "example.net"}
    ]
    
    # Example clicks and conversions data
    clicks_conversions = [
        {"user_id": "user1234", "timestamp": datetime.datetime.now(), "ad_campaign_id": 3, "conversion_type": "signup"},
        {"user_id": "user4566", "timestamp": datetime.datetime.now(), "ad_campaign_id": 4, "conversion_type": "purchase"}
    ]
    client.publish_json(impressions_topic, ad_impressions)
    client.publish_json(clicks_conversions_topic, clicks_conversions)
    topic_list = [impressions_topic, clicks_conversions_topic, bid_requests_topic]
    client.subscribe_to_topics(topic_list)