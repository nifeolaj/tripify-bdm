from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import json
import unicodedata
import re
import logging
import io
import pytz
import os
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def clean_text(text):
    """
    Normalizes text by:
    1. Removing special characters/diacritics
    2. Converting to lowercase
    3. Removing extra whitespace
    4. Preserving basic punctuation
    """
    # Normalize unicode characters
    text = unicodedata.normalize('NFKD', str(text))
    # Remove non-ASCII characters
    text = text.encode('ascii', 'ignore').decode('ascii')
    # Remove special characters except basic punctuation
    text = re.sub(r'[^a-zA-Z0-9\s.,!?-]', '', text)
    # Convert to lowercase
    text = text.lower()
    # Remove extra whitespace
    text = re.sub('\s+', ' ', text).strip()
    return text

def clean_itinerary_data(content):
    """
    Cleans itinerary data from raw content
    """
    try:
        # Remove JSON code blocks if present
        cleaned_content = content.replace('```json', '').replace('```', '').strip()
        
        # Load JSON data
        itineraries = json.loads(cleaned_content)
        
        # Clean each entry
        for entry in itineraries:
            for key in entry:
                entry[key] = clean_text(entry[key])
        
        return itineraries
    except Exception as e:
        logging.error(f"Error cleaning itinerary data: {str(e)}")
        raise

class GCSLogHandler:
    def __init__(self, bucket, gcs_hook, dag_id):
        self.bucket = bucket
        self.storage_client = gcs_hook.get_conn()
        self.dag_id = dag_id
        self.log_buffer = io.StringIO()
        
    def write(self, message):
        self.log_buffer.write(message)
        
    def flush(self):
        pass
        
    def save_logs(self, context):
        log_content = self.log_buffer.getvalue()
        current_time = datetime.now(pytz.UTC)
        timestamp = current_time.strftime("%Y_%m_%d_%H_%M_%S")
        log_path = f"trusted_zone/logs/{timestamp}.log"
        
        metadata = f"""
Task Run Metadata:
----------------
DAG ID: {self.dag_id}
Task ID: {context['task'].task_id}
Logical Date: {context['logical_date']}
Start Date: {context['task_instance'].start_date}
End Date: {current_time}
----------------
"""
        full_log_content = metadata + "\n" + log_content
        
        bucket = self.storage_client.bucket(self.bucket)
        blob = bucket.blob(log_path)
        blob.upload_from_string(full_log_content, content_type='text/plain')
        self.log_buffer.close()

def capture_airflow_logs(bucket_name, dag_id, context):
    """Capture and store Airflow logs in GCS."""
    gcs_hook = GCSHook()
    
    root_logger = logging.getLogger()
    
    gcs_handler = GCSLogHandler(bucket_name, gcs_hook, dag_id)
    stream_handler = logging.StreamHandler(gcs_handler)
    
    formatter = logging.Formatter(
        '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
        '%Y-%m-%d, %H:%M:%S UTC'
    )
    stream_handler.setFormatter(formatter)
    
    root_logger.addHandler(stream_handler)
    
    return gcs_handler

def process_itineraries(
    bucket_name,
    **context
):
    # Initialize GCSHook
    gcs_hook = GCSHook()
    storage_client = gcs_hook.get_conn()
    bucket = storage_client.bucket(bucket_name)
    
    # Setup logging to GCS
    gcs_handler = capture_airflow_logs(bucket_name, context['dag'].dag_id, context)
    logger = logging.getLogger(f"airflow.task.{context['task'].task_id}")
    
    try:
        # Get today's date in the format used in GCS
        today = datetime.now().strftime("%Y_%m_%d")
        logger.info(f"Starting processing for date: {today}")
        
        # Define supported cities
        cities = ['barcelona', 'madrid', 'paris']
        
        # Process each city's data
        for city in cities:
            logger.info(f"Processing itineraries for city: {city}")
            
            # Check for today's data
            city_prefix = f"landing_zone/llm_itineraries/{city}/{today}/"
            logger.info(f"Checking for itinerary data in: {city_prefix}")
            
            try:
                blobs = bucket.list_blobs(prefix=city_prefix)
                txt_blobs = [blob for blob in blobs if blob.name.endswith('.txt')]
                
                if not txt_blobs:
                    logger.info(f"No itinerary data found for {city} on {today}")
                    continue
                    
                logger.info(f"Found {len(txt_blobs)} itinerary files to process for {city}")
                
                # Process each file and collect all itineraries
                city_itineraries = []
                
                for blob in txt_blobs:
                    try:
                        # Extract metadata from filename
                        filename = blob.name.split('/')[-1]
                        logger.info(f"Processing file: {filename}")
                        
                        # Download and read content
                        content = blob.download_as_text()
                        logger.info(f"Downloaded content from {filename}")
                        
                        # Clean and process the data
                        processed_itineraries = clean_itinerary_data(content)
                        logger.info(f"Successfully cleaned itinerary data from {filename}")
                        
                        # Add metadata to each itinerary
                        filename_parts = filename.replace('.txt', '').split('_')
                        metadata = {
                            'day_number': filename_parts[5],
                            'user_persona': '_'.join(filename_parts[6:])
                        }
                        
                        for itinerary in processed_itineraries:
                            itinerary.update(metadata)
                        
                        city_itineraries.extend(processed_itineraries)
                        logger.info(f"Added {len(processed_itineraries)} itineraries from {filename}")
                        
                    except Exception as e:
                        logger.error(f"Error processing file {filename}: {str(e)}")
                        continue
                
                if city_itineraries:
                    # Save processed itineraries to trusted zone as Parquet
                    df = pd.DataFrame(city_itineraries)
                    parquet_buffer = io.BytesIO()
                    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
                    parquet_buffer.seek(0)
                    output_blob = bucket.blob(f"trusted_zone/itineraries/{city}/{today}_itineraries.parquet")
                    output_blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')
                    logger.info(f"Successfully saved {len(city_itineraries)} processed itineraries for {city} as Parquet")
                
            except Exception as e:
                logger.error(f"Error processing city {city}: {str(e)}")
                raise
    
    except Exception as e:
        logger.error(f"Error in process_itineraries: {str(e)}")
        raise
    finally:
        # Save logs to GCS
        gcs_handler.save_logs(context)

with DAG(
    'gcs_landing_to_trusted_itineraries',
    start_date=datetime(2025, 5, 11),
    description='DAG to process itinerary data from landing zone into trusted zone.',
    tags=['gcs', 'itineraries'],
    schedule='@daily',
    catchup=False
) as dag:
    
    gcs_bucket_name = Variable.get("events_gcs_bucket_name", default_var="bdm-project")
    
    process_itineraries_task = PythonOperator(
        task_id='process_itineraries',
        python_callable=process_itineraries,
        op_kwargs={
            'bucket_name': gcs_bucket_name
        }
    ) 