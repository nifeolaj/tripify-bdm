from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import duckdb
import os
import logging
import io
import pytz
from event_processing.clean_transport_options import (
    preprocess_bus, preprocess_train, preprocess_air, init_spark_session
)
import tempfile
from google.cloud import storage

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

def process_transport_type(transport_type, **context):
    """Process transport data for a specific type using Databricks"""
    gcs_hook = GCSHook()
    execution_date = context['logical_date']
    date_str = execution_date.strftime('%Y%m%d')
    
    logging.info(f"Starting processing for transport type: {transport_type}")
    logging.info(f"Execution date: {execution_date}")
    
    # Download data from GCS
    today = datetime.now().strftime("%Y%m%d")
    landing_path = f'landing_zone/transport/{transport_type}fare/{today}'
    logging.info(f"Looking for files in landing path: {landing_path}")
    
    # List all blobs with the prefix
    blob_names = gcs_hook.list(LANDING_BUCKET, prefix=landing_path)
    logging.info(f"Found {len(blob_names)} items in landing path")
    
    if not blob_names:
        logging.info(f"No files found in {landing_path}")
        return
    
    # Filter for actual parquet files (not directories)
    parquet_files = [blob for blob in blob_names if blob.endswith('.parquet')]
    logging.info(f"Found {len(parquet_files)} parquet files to process")
    
    if not parquet_files:
        logging.info("No parquet files found to process")
        return
    
    # Process each Parquet file
    for blob_name in parquet_files:
        logging.info(f"Processing file: {blob_name}")
        
        # Create GCS paths for input and output
        input_gcs_path = f'gs://{LANDING_BUCKET}/{blob_name}'
        output_gcs_path = f'gs://{TRUSTED_BUCKET}/trusted_zone/transport/{transport_type}fare/{date_str}/{os.path.basename(blob_name)}'
        
        logging.info(f"Input GCS path: {input_gcs_path}")
        logging.info(f"Output GCS path: {output_gcs_path}")
        
        # Create Databricks task
        logging.info("Creating Databricks task")
        databricks_task = DatabricksSubmitRunOperator(
            task_id=f'process_{transport_type}_{os.path.basename(blob_name)}',
            databricks_conn_id='databricks_default',
            existing_cluster_id='0525-225010-u4og1d5d',
            notebook_task={
                'notebook_path': NOTEBOOK_PATH,
                'base_parameters': {
                    'input_path': input_gcs_path,
                    'output_path': output_gcs_path,
                    'transport_type': transport_type
                }
            },
            dag=dag
        )
        
        # Execute the task
        logging.info("Executing Databricks task")
        try:
            databricks_task.execute(context)
            logging.info("Databricks task completed successfully")
        except Exception as e:
            logging.error(f"Error executing Databricks task: {str(e)}")
            raise
    
    logging.info(f"Completed processing for transport type: {transport_type}")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'databricks_landing_to_trusted_transport',
    default_args=default_args,
    description='Process transport data using Databricks',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# GCS bucket names
LANDING_BUCKET = 'bdm-project'
TRUSTED_BUCKET = 'bdm-project'

# Databricks notebook path
NOTEBOOK_PATH = '/Workspace/Repos/travelwithtripify@gmail.com/tripify/process_transport'

# Create tasks for each transport type
air_task = PythonOperator(
    task_id='process_air',
    python_callable=process_transport_type,
    op_kwargs={'transport_type': 'air'},
    dag=dag
)

bus_task = PythonOperator(
    task_id='process_bus',
    python_callable=process_transport_type,
    op_kwargs={'transport_type': 'bus'},
    dag=dag
)

train_task = PythonOperator(
    task_id='process_train',
    python_callable=process_transport_type,
    op_kwargs={'transport_type': 'train'},
    dag=dag
)

# Set task dependencies
[air_task, bus_task, train_task] 