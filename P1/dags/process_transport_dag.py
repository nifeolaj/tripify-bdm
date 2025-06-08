from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime
import re
from google.cloud import storage

def generate_date():
    return datetime.now().strftime('%Y_%m_%d')

def generate_timestamp():
    return datetime.now().strftime('%Y%m%d_%H%M%S')

def extract_transport(filename):
    match = re.search(r'data_([a-zA-Z]+?)_\d{8}_\d+\.parquet$', filename)
    return match.group(1) if match else "unknown"

def process_files(ti, bucket_name):
    # Get the list of files from XCom
    files = ti.xcom_pull(task_ids='list_files')
    if not files:
        print("No files found to process")
        return []
    
    # Initialize GCS client
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    storage_client = gcs_hook.get_conn()
    bucket = storage_client.bucket(bucket_name)

    # Generate Timestamp
    date_prefix = generate_timestamp()
    
    processed_files = []
    for filename in files:
        # Extract filename from the full path
        source_blob_name = filename
        file_name = source_blob_name.split('/')[-1]
        
        # Extract transport method from filename
        transport_method = extract_transport(file_name)
        
        # Define destination path
        destination_blob_name = f"landing_zone/transport/{transport_method}/{date_prefix}/{file_name}"
        
        # Copy the file
        source_blob = bucket.blob(source_blob_name)
        destination_blob = bucket.blob(destination_blob_name)
        
        # Copy source blob to destination
        blob_copy = bucket.copy_blob(
            source_blob, bucket, destination_blob_name
        )
        
        processed_files.append(destination_blob_name)
        print(f"Processed file: {source_blob_name} to {destination_blob_name}")
    
    return processed_files

with DAG(
    'gcs_transport_processing',
    start_date=datetime(2025, 3, 29),
    description='DAG to process transport data to persistent storage',
    tags=['gcs_transport_processing'],
    schedule='@daily',
    catchup=False
) as dag:
    # Define the GCS bucket name
    gcs_bucket_name = "bdm-project"
    
    # Use templated variable for date prefix
    date_prefix = '{{ execution_date.strftime("%Y_%m_%d") }}'
    
    # Sensor to check for files with current date prefix
    check_prefix = GCSObjectsWithPrefixExistenceSensor(
        task_id='check_prefix',
        bucket=gcs_bucket_name,
        prefix=f'tmp_landing_zone/kafka/{date_prefix}',
        poke_interval=60,
        timeout=3600,
        mode='reschedule',
        soft_fail=True
    )
    
    # List GCS objects with the specified prefix
    list_files = GCSListObjectsOperator(
        task_id='list_files',
        bucket=gcs_bucket_name,
        prefix=f'tmp_landing_zone/kafka/{date_prefix}'
    )
    
    # Task to process all files
    process_files_task = PythonOperator(
        task_id='process_files',
        python_callable=process_files,
        op_kwargs={'bucket_name': gcs_bucket_name},
        provide_context=True
    )
    
    # Define task dependencies
    check_prefix >> list_files >> process_files_task
    
    # Dataset for cross-DAG triggering
    dag.dataset = Dataset(f"gs://{gcs_bucket_name}/tmp_landing_zone/kafka/{generate_date()}")