from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import logging

logging.basicConfig(level=logging.INFO)


class KafkaManager():

    def __init__(self):
        
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            retries=5,
            acks='all'
        )
    
    def safe_send(self,producer, topic, data):
        try:
            future = producer.send(topic, data)
            future.get(timeout=10)
            logging.info(f"Sent data to {topic}: {data}")
        except KafkaError as e:
            logging.error(f"Failed to send message: {e}")

    def scrape_and_send(self,topic,data):
        try:
            self.safe_send(self.kafka_producer, topic, data)
        finally:
            self.kafka_producer.flush()