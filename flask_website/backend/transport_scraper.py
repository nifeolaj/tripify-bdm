from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import logging
from datetime import datetime, timedelta
import requests  # For making HTTP requests
import os

logger = logging.getLogger(__name__)

class TransportScraper:
    def __init__(self, kafka_bootstrap_servers=None, scraper_server_url=None):
        self.bootstrap_servers = kafka_bootstrap_servers or [os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')]
        self.scraper_server_url = scraper_server_url or os.getenv('SCRAPER_SERVER_URL', 'http://localhost:8000')
        self.kafka_available = False
        
        try:
            # Initialize Kafka producer
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            self.kafka_available = True
        except NoBrokersAvailable:
            logger.warning("Kafka brokers not available. Will continue without Kafka.")
            self.kafka_available = False
        except Exception as e:
            logger.warning(f"Error connecting to Kafka: {str(e)}. Will continue without Kafka.")
            self.kafka_available = False
        
    def scrape_transport_options(self, from_city, to_city, date):
        """Request transport options from the scraper server"""
        try:
            # Make request to scraper server
            response = requests.post(
                f"{self.scraper_server_url}/scrape",
                json={
                    'from_city': from_city,
                    'to_city': to_city,
                    'date': date
                }
            )
            
            if response.status_code == 200:
                logger.info(f"Successfully requested scraping for {from_city} to {to_city} on {date}")
                return True
            else:
                logger.error(f"Failed to request scraping. Status code: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error requesting transport options: {str(e)}")
            return False 