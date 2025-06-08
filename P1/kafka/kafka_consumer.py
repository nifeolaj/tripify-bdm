from kafka import KafkaConsumer
import json
import logging
import pandas as pd
import time
import os
from google.cloud import storage
from datetime import datetime
from path import OUTPUT_DIR

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
BATCH_SIZE = 1000  # Number of messages per Parquet file
SAVE_INTERVAL = 300  # Seconds between saves (5 minutes)
TOPIC_NAME = "trainfare"
BUCKET_NAME = "bdm-project"  # GCS bucket name
SOURCE_NAME = "kafka"   # Source name for GCS path

def ensure_output_dir():
    """Create output directory if it doesn't exist"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def upload_to_gcs(local_file_path):
    """Upload a file to Google Cloud Storage"""
    try:
        # Extract file name
        file_name = os.path.basename(local_file_path)

        # Get current date for partitioning
        now = datetime.now()
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")

        # Create the destination path
        destination_blob_name = f"tmp_landing_zone/kafka/{year}_{month}_{day}_{file_name}"

        # Initialize GCS client
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(destination_blob_name)

        # Upload the file
        blob.upload_from_filename(local_file_path)
        logger.info(f"File {local_file_path} uploaded to gs://{BUCKET_NAME}/{destination_blob_name}")
        return True
    except Exception as e:
        logger.error(f"Failed to upload file to GCS: {e}")
        return False

def write_parquet(batch):
    """Write batch of messages to Parquet file and upload to GCS"""
    try:
        df = pd.DataFrame(batch)
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{OUTPUT_DIR}/data_{TOPIC_NAME}_{timestamp}.parquet"
        
        # Write locally
        df.to_parquet(filename, engine='pyarrow')
        logger.info(f"Saved {len(batch)} messages to {filename}")
        
        # Upload to GCS
        upload_success = upload_to_gcs(filename)
        if upload_success:
            logger.info(f"Successfully uploaded {filename} to GCS")
        else:
            logger.warning(f"Failed to upload {filename} to GCS")
            
    except Exception as e:
        logger.error(f"Failed to write Parquet file: {str(e)}")

def process_batch(batch):
    """Process and save batch of messages"""
    if not batch:
        return
    
    try:
        write_parquet(batch)
    except Exception as e:
        logger.error(f"Batch processing failed: {str(e)}")

def start_consumer():
    ensure_output_dir()
    
    # Configure consumer for latest messages
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',  # Change to 'earliest' for historical data
        enable_auto_commit=True,     # Auto-commit offsets
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        max_poll_records=500         # Max messages per poll
    )

    batch = []
    last_save_time = time.time()

    try:
        while True:
            # Poll for messages with timeout
            messages = consumer.poll(timeout_ms=1000)
            
            for _, msg_list in messages.items():
                for message in msg_list:
                    try:
                        batch.append(message.value)
                    except Exception as e:
                        logger.error(f"Error processing message: {str(e)}")

            # Check batch size or time interval
            current_time = time.time()
            if len(batch) >= BATCH_SIZE or (current_time - last_save_time) >= SAVE_INTERVAL:
                process_batch(batch)
                batch = []
                last_save_time = current_time

    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
        # Process remaining messages before exit
        if batch:
            process_batch(batch)
    finally:
        consumer.close()

if __name__ == "__main__":
    start_consumer()