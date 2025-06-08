from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import duckdb
import os
import logging
import io
import pytz
from event_processing.clean_transport_options import (
    preprocess_bus, preprocess_train, preprocess_air, init_spark_session
)

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

def process_transport_type(
    transport_type,
    bucket_name,
    gcs_prefix,
    **context
):
    # Initialize GCSHook
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    storage_client = gcs_hook.get_conn()
    bucket = storage_client.bucket(bucket_name)
    
    # Setup logging to GCS
    gcs_handler = capture_airflow_logs(bucket_name, context['dag'].dag_id, context)
    logger = logging.getLogger(f"airflow.task.{context['task'].task_id}")
    
    try:
        # Get today's date in the format used in GCS (YYYYMMDD)
        today = datetime.now().strftime("%Y%m%d")
        logger.info(f"Starting processing for transport type: {transport_type} on date: {today}")
        
        # List all blobs with the given prefix for this transport type
        transport_prefix = f"{gcs_prefix}{transport_type}fare/"
        blobs = bucket.list_blobs(prefix=transport_prefix)
        
        # Filter for today's data folders
        today_folders = set()
        for blob in blobs:
            # Extract date folder from path like "transport/airfare/20250405_035333/"
            parts = blob.name.split('/')
            if len(parts) >= 3:
                folder_name = parts[3]
                # Match YYYYMMDD_XXXXXX format
                if folder_name.startswith(today) and '_' in folder_name:
                    today_folders.add(folder_name)
        
        if not today_folders:
            logger.warning(f"No data folders found for {transport_type} on {today}")
            return

        # Local path for DuckDB database
        local_db_path = f'/tmp/transport_{transport_type}.duckdb'
        
        # Try to download existing DuckDB file from GCS
        try:
            existing_db_blob = bucket.blob(f'trusted_zone/transport_{transport_type}.duckdb')
            logger.info(f"Checking for existing database at: trusted_zone/transport_{transport_type}.duckdb")
            
            if existing_db_blob.exists():
                logger.info(f"Found existing transport_{transport_type}.duckdb in GCS, downloading...")
                existing_db_blob.download_to_filename(local_db_path)
                logger.info(f"Successfully downloaded existing database to {local_db_path}")
            else:
                logger.info(f"No existing database found for {transport_type}, will create new one")
        except Exception as e:
            logger.error(f"Error checking/downloading existing database: {str(e)}")
            logger.info("Will proceed with new database creation")
        
        # Setup DuckDB connection
        logger.info(f"Connecting to DuckDB at {local_db_path}")
        con = duckdb.connect(database=local_db_path)
        
        # Create sequence first
        logger.info("Creating/checking sequence...")
        con.execute(f"""
            CREATE SEQUENCE IF NOT EXISTS transport_{transport_type}_id_seq;
        """)
        
        # Create table if not exists 
        logger.info("Creating/checking table schema...")
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS transport_{transport_type} (
                id INTEGER PRIMARY KEY DEFAULT nextval('transport_{transport_type}_id_seq'),
                type TEXT,
                company TEXT,
                departure TEXT,
                departure_country TEXT,
                departure_station TEXT,
                arrival TEXT,
                arrival_country TEXT,
                arrival_station TEXT,
                departure_time TIMESTAMP WITH TIME ZONE,
                arrival_time TIMESTAMP WITH TIME ZONE,
                duration TEXT,
                price TEXT,
                currency TEXT
            );
        """)
        
        # Verify table exists and show count
        logger.info("Verifying database setup...")
        try:
            result = con.execute(f"SELECT COUNT(*) FROM transport_{transport_type}").fetchone()
            logger.info(f"Current record count in transport_{transport_type}: {result[0]}")
        except Exception as e:
            logger.error(f"Error verifying table: {str(e)}")
        
        files_processed = False
        
        # Process each folder from today
        for folder in today_folders:
            folder_prefix = f"{transport_prefix}{folder}/"
            folder_blobs = bucket.list_blobs(prefix=folder_prefix)
            parquet_blobs = [blob for blob in folder_blobs if blob.name.endswith('.parquet')]
            
            logger.info(f"Processing {len(parquet_blobs)} files from folder {folder}")
            
            # Process each Parquet file in the folder
            for blob in parquet_blobs:
                local_file = f"/tmp/{os.path.basename(blob.name)}"
                logger.info(f"Processing {blob.name}")

                try:
                    # Download file locally
                    blob.download_to_filename(local_file)
                    
                    # Process with Spark
                    spark_task = SparkSubmitOperator(
                        task_id=f'spark_process_{transport_type}_{os.path.basename(blob.name)}',
                        application=__file__,  # Use the current file
                        conn_id='spark_default',
                        verbose=True,
                        application_args=[
                            '--input', local_file,
                            '--output', f'gs://{bucket_name}/trusted_zone/transport_{transport_type}.parquet',
                            '--transport_type', transport_type,
                            '--mode', 'process'  # Add mode to indicate we want to process
                        ],
                        conf={
                            'spark.master': 'spark://spark-master:7077',
                            'spark.submit.deployMode': 'cluster',
                            'spark.executor.memory': '2g',
                            'spark.driver.memory': '1g'
                        }
                    )
                    
                    # Execute Spark task
                    spark_task.execute(context)
                    
                    files_processed = True
                    logger.info(f"Successfully processed {blob.name}")

                except Exception as e:
                    logger.error(f"Error processing {blob.name}: {str(e)}")
                    raise e
                finally:
                    # Clean up the temporary file
                    if os.path.exists(local_file):
                        os.remove(local_file)

        # Close the DuckDB connection
        con.close()

        # Upload the DuckDB file to GCS if we processed any files
        if files_processed:
            destination_blob = bucket.blob(f'trusted_zone/transport_{transport_type}.duckdb')
            try:
                destination_blob.upload_from_filename(local_db_path)
                logger.info(f"Successfully uploaded transport_{transport_type}.duckdb to GCS")
            finally:
                # Clean up the temporary file
                if os.path.exists(local_db_path):
                    os.remove(local_db_path)
        else:
            logger.info("No files were processed, skipping upload")
            
    except Exception as e:
        logger.error(f"Error in process_transport_type: {str(e)}")
        raise e
    finally:
        # Save logs to GCS
        gcs_handler.save_logs(context)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, help='Input Parquet file path')
    parser.add_argument('--output', required=True, help='Output Parquet file path')
    parser.add_argument('--transport_type', required=True, help='Transport type (air/bus/train)')
    parser.add_argument('--mode', required=True, help='Mode to run in (process)')
    args = parser.parse_args()

    if args.mode == 'process':
        # Initialize Spark session
        spark = init_spark_session()

        try:
            # Read input Parquet file
            df = spark.read.parquet(args.input)

            # Select preprocessing function based on transport type
            preprocess_func = {
                'air': preprocess_air,
                'bus': preprocess_bus,
                'train': preprocess_train
            }[args.transport_type]

            # Process the data
            processed_df = preprocess_func(df)

            # Write the processed data
            processed_df.write.mode('append').parquet(args.output)

        finally:
            spark.stop()

with DAG(
    'gcs_landing_to_trusted_transport',
    start_date=datetime(2025, 5, 11),
    description='DAG to load transport Parquet files from landing zone into DuckDB database in the trusted zone.',
    tags=['gcs', 'duckdb', 'transport'],
    schedule='@daily',
    catchup=False
) as dag:
    gcs_bucket_name = "bdm-project"
    gcs_prefix = "landing_zone/transport/"

    # Create tasks for each transport type
    for transport_type in ['air', 'bus', 'train']:
        duckdb_gcs_path = f"gs://bdm-project/trusted_zone/transport_{transport_type}.duckdb"
        
        process_task = PythonOperator(
            task_id=f'process_{transport_type}_transport',
            python_callable=process_transport_type,
            op_kwargs={
                'transport_type': transport_type,
                'bucket_name': gcs_bucket_name,
                'gcs_prefix': gcs_prefix
            }
        ) 