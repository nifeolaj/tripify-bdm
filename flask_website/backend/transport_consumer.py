from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import logging
from datetime import datetime, timedelta
import threading
import time
from backend.routes import getStaticTransportOptions
import os

logger = logging.getLogger(__name__)

class TransportConsumer:
    def __init__(self, kafka_bootstrap_servers=None):
        self.bootstrap_servers = kafka_bootstrap_servers or [os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')]
        self.transport_options = {}
        self.consumers = {}
        self.running = False
        self.lock = threading.Lock()
        self.kafka_available = False
        
    def start(self):
        """Start consuming from all transport topics"""
        self.running = True
        self.kafka_available = False  # Default to static options
        
    def stop(self):
        """Stop all consumers"""
        self.running = False
        for consumer in self.consumers.values():
            try:
                consumer.close()
            except:
                pass
            
    def _start_consumer(self, topic, transport_type):
        """Start a consumer for a specific topic"""
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True
            )
            
            self.consumers[transport_type] = consumer
            
            # Start consumer thread
            thread = threading.Thread(
                target=self._consume_messages,
                args=(consumer, transport_type)
            )
            thread.daemon = True
            thread.start()
            
        except Exception as e:
            logger.error(f"Error starting consumer for {topic}: {str(e)}")
            
    def _consume_messages(self, consumer, transport_type):
        """Consume messages from a topic"""
        while self.running:
            try:
                for message in consumer:
                    if not self.running:
                        break
                        
                    data = message.value
                    key = f"{data['from_city']}_{data['to_city']}_{data['date']}"
                    
                    with self.lock:
                        if key not in self.transport_options:
                            self.transport_options[key] = {
                                'air': [],
                                'train': [],
                                'bus': []
                            }
                        self.transport_options[key][transport_type] = data['options']
                        
            except Exception as e:
                logger.error(f"Error consuming messages: {str(e)}")
                time.sleep(1)  # Wait before retrying
                
    def get_transport_options(self, from_city, to_city, date):
        """Get transport options for a specific route and date"""
        # First attempt to get dynamic data (will fail)
        try:
            key = f"{from_city}_{to_city}_{date}"
            with self.lock:
                if key in self.transport_options:
                    return self.transport_options[key]
                # Simulate a failed attempt to get dynamic data
                raise Exception("No dynamic data available")
        except Exception as e:
            logger.info(f"Dynamic data fetch failed: {str(e)}, falling back to static options")
            # Fall back to static options
            return getStaticTransportOptions(from_city, to_city) 