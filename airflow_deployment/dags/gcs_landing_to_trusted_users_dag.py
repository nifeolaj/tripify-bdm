from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import io
import pytz
from airflow.models import Variable
import pandas as pd
import json
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

def copy_landing_to_trusted(bucket_name, **context):
    """Copy data from landing zone to trusted zone, converting CSV to Parquet format."""
    gcs_hook = GCSHook()
    storage_client = gcs_hook.get_conn()
    bucket = storage_client.bucket(bucket_name)

    gcs_handler = capture_airflow_logs(bucket_name, context['dag'].dag_id, context)
    logger = logging.getLogger(f"airflow.task.{context['task'].task_id}")

    try:
        # Get today's date in the format YYYY-MM-DD
        today = datetime.now().strftime("%Y-%m-%d")

        # Define source and destination paths
        landing_path = f"landing_zone/users/{today}/"
        trusted_path = f"trusted_zone/users/{today}/"

        # List of files to process
        files_to_process = ['users.csv', 'posts.csv', 'likes.csv']

        for file_name in files_to_process:
            logger.info(f"Processing {file_name}...")
            
            # Download CSV file from landing zone
            source_blob = bucket.blob(landing_path + file_name)
            csv_content = source_blob.download_as_string()
            
            # Convert CSV to DataFrame
            df = pd.read_csv(io.BytesIO(csv_content))
            
            # Convert to Parquet format
            parquet_file_name = file_name.replace('.csv', '.parquet')
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)
            
            # Upload Parquet file to trusted zone
            destination_blob = bucket.blob(trusted_path + parquet_file_name)
            destination_blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')
            
            logger.info(f"Successfully converted and uploaded {parquet_file_name} to trusted zone")

        logger.info("All files processed successfully")

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    finally:
        gcs_handler.save_logs(context)

with DAG(
    'gcs_landing_to_trusted_users_dag',
    start_date=datetime(2025, 5, 11),
    description='DAG to copy data from landing zone to trusted zone, converting CSV to Parquet format.',
    tags=['gcs', 'users', 'trusted'],
    schedule='@daily',
    catchup=False
) as dag:
    gcs_bucket_name = Variable.get("events_gcs_bucket_name", default_var="bdm-project")

    copy_task = PythonOperator(
        task_id='copy_landing_to_trusted',
        python_callable=copy_landing_to_trusted,
        op_kwargs={'bucket_name': gcs_bucket_name}
    ) 