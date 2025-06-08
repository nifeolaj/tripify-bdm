import requests
import os
import pandas as pd
import logging
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator  
# Google Cloud Storage
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
# Airflow logging
from airflow.utils.log.logging_mixin import LoggingMixin

logging = LoggingMixin().log

def download_events(url, save_path):
    logging.info(f"Starting download: {url}")
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(save_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=1024):
                file.write(chunk)
        logging.info(f"Successfully downloaded: {save_path}")
    else:
        logging.error(f"Failed to download {url}. Status code: {response.status_code}")

def clean_csv(file_path):
    if "madrid" in file_path or "paris" in file_path:
        try:
            logging.info(f"Cleaning CSV file: {file_path}")
            df = pd.read_csv(file_path, delimiter=';', dtype=str, encoding="ISO-8859-1")  # Handle encoding
            df.to_csv(file_path, index=False)
            logging.info(f"Successfully cleaned: {file_path}")
        except Exception as e:
            logging.error(f"Error cleaning {file_path}: {e}")

with DAG('fetch_events', start_date=datetime(2025, 3, 29),
         description='DAG to download events data', tags=['events'],
         schedule='@daily', catchup=False):
    
    # Define the GCS bucket name
    gcs_bucket_name = "bdm-project" 
    
    urls = {
        "paris.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/que-faire-a-paris-/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B",
        "madrid.csv": "https://datos.madrid.es/egob/catalogo/300107-0-agenda-actividades-eventos.csv",
        "barcelona.csv": "https://opendata-ajuntament.barcelona.cat/data/dataset/877ccf66-9106-4ae2-be51-95a9f6469e4c/resource/877ccf66-9106-4ae2-be51-95a9f6469e4c/download"
    }
    
    os.makedirs("events_data_csv", exist_ok=True)
    
    download_tasks = []
    clean_tasks = []
    upload_tasks = []
    
    for filename, url in urls.items():
        save_path = os.path.join("events_data_csv", filename)
        
        task_download_events = PythonOperator(task_id=f'download_{filename.replace(".csv", "")}', python_callable=download_events, op_kwargs={'url': url, 'save_path': save_path})
        task_clean_events = PythonOperator(task_id=f'clean_{filename.replace(".csv", "")}', python_callable=clean_csv, op_kwargs={'file_path': save_path})

        destination_filename = "tmp_landing_zone/events/{{ execution_date.strftime('%Y_%m_%d').lower() }}_" + filename
        
        task_upload_to_gcs = LocalFilesystemToGCSOperator(
            task_id=f'upload_{filename.replace(".csv", "")}',
            src=save_path,
            dst=destination_filename,
            bucket=gcs_bucket_name,
            gcp_conn_id="google_cloud_default"  
        )

        download_tasks.append(task_download_events)
        clean_tasks.append(task_clean_events)
        upload_tasks.append(task_upload_to_gcs)
        
        # Set dependencies
        task_download_events >> task_clean_events >> task_upload_to_gcs