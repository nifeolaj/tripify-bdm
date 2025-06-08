from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import os
import logging
import io
import pytz
from event_processing.clean_barcelona_events import clean_barcelona_events
from event_processing.clean_madrid_events import clean_madrid_events
from event_processing.clean_paris_events import clean_paris_events
import pandas as pd

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

def process_events(
    bucket_name,
    gcs_prefix,
    **context
):
    gcs_hook = GCSHook()
    storage_client = gcs_hook.get_conn()
    bucket = storage_client.bucket(bucket_name)
    gcs_handler = capture_airflow_logs(bucket_name, context['dag'].dag_id, context)
    logger = logging.getLogger(f"airflow.task.{context['task'].task_id}")
    try:
        today = datetime.now().strftime("%Y_%m_%d")
        city_processors = {
            'barcelona': clean_barcelona_events,
            'madrid': clean_madrid_events,
            'paris': clean_paris_events
        }
        for city, process_func in city_processors.items():
            logger.info(f"Processing city: {city}")
            city_prefix = f"{gcs_prefix}{city}/{today}/"
            logger.info(f"Checking for data in: {city_prefix}")
            try:
                blobs = bucket.list_blobs(prefix=city_prefix)
                csv_blobs = [blob for blob in blobs if blob.name.endswith('.csv')]
                if not csv_blobs:
                    logger.info(f"No data found for {city} on {today}")
                    continue
                logger.info(f"Found {len(csv_blobs)} files to process for {city}")
                for blob in csv_blobs:
                    local_file = f"/tmp/{os.path.basename(blob.name)}"
                    try:
                        logger.info(f"Downloading {blob.name} to {local_file}")
                        blob.download_to_filename(local_file)
                        logger.info(f"Processing {blob.name}")
                        processed_df = process_func(local_file)
                        logger.info(f"Successfully processed file, got {len(processed_df)} records")
                        base_filename = os.path.splitext(os.path.basename(blob.name))[0]
                        local_csv = f"/tmp/{base_filename}_processed.csv"
                        local_parquet = f"/tmp/{base_filename}_processed.parquet"
                        processed_df.to_csv(local_csv, index=False)
                        processed_df.to_parquet(local_parquet, index=False)
                        logger.info(f"Saved processed data as CSV: {local_csv} and Parquet: {local_parquet}")
                        gcs_output_prefix = f"trusted_zone/events/{city}/{today}/"
                        csv_blob = bucket.blob(gcs_output_prefix + os.path.basename(local_csv))
                        parquet_blob = bucket.blob(gcs_output_prefix + os.path.basename(local_parquet))
                        csv_blob.upload_from_filename(local_csv)
                        parquet_blob.upload_from_filename(local_parquet)
                        logger.info(f"Uploaded CSV and Parquet to GCS: {gcs_output_prefix}")
                    except Exception as e:
                        logger.error(f"Error processing {blob.name} for {city}: {str(e)}")
                        raise e
                    finally:
                        if os.path.exists(local_file):
                            logger.info(f"Cleaning up temporary file: {local_file}")
                            os.remove(local_file)
                        if os.path.exists(local_csv):
                            logger.info(f"Cleaning up temporary CSV file: {local_csv}")
                            os.remove(local_csv)
                        if os.path.exists(local_parquet):
                            logger.info(f"Cleaning up temporary Parquet file: {local_parquet}")
                            os.remove(local_parquet)
            except Exception as e:
                logger.error(f"Error processing city {city}: {str(e)}")
                raise
    except Exception as e:
        logger.error(f"Error in process_events: {str(e)}")
        raise
    finally:
        gcs_handler.save_logs(context)

with DAG(
    'gcs_landing_to_trusted_events',
    start_date=datetime(2025, 5, 11),
    description='DAG to load events data from landing zone into trusted zone as CSV and Parquet.',
    tags=['gcs', 'events', 'csv', 'parquet'],
    schedule='@daily',
    catchup=False
) as dag:
    
    # Get configuration from Airflow Variables
    gcs_bucket_name = Variable.get("events_gcs_bucket_name", default_var="bdm-project")
    gcs_prefix = Variable.get("events_gcs_prefix", default_var="landing_zone/events/")
    
    process_events_task = PythonOperator(
        task_id='process_events',
        python_callable=process_events,
        op_kwargs={
            'bucket_name': gcs_bucket_name,
            'gcs_prefix': gcs_prefix
        }
    ) 