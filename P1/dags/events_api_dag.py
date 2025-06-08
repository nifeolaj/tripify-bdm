import os
import json
import logging
import requests
from serpapi import GoogleSearch
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator    
# Google Cloud Storage
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("events_data_fetcher.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("EventsDataFetcher")

# Create data directory if it doesn't exist
os.makedirs("events_data", exist_ok=True)

# Airflow logging
from airflow.utils.log.logging_mixin import LoggingMixin
logging = LoggingMixin().log

def fetch_barcelona_events():
    """Fetch events data for Barcelona and save as both CSV and JSON without filtering"""
    url = "https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search"
    params = {
        "resource_id": "877ccf66-9106-4ae2-be51-95a9f6469e4c",
        "limit": 100
    }
    
    logger.info("Fetching Barcelona events data...")
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    data = response.json()
    records = data.get('result', {}).get('records', [])
    
    if records:
        # Save as JSON 
        json_filename = os.path.join("events_data", f"barcelona_events.json")
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(records, f, indent=2, ensure_ascii=False)
        logger.info(f"Barcelona events data saved to {json_filename} (JSON format)")
        
        return json_filename
    else:
        logger.warning("No Barcelona events data found")
        return None
    

def fetch_paris_events():
    """Fetch events data for Paris (JSON format)"""
    url = "https://opendata.paris.fr/api/v2/catalog/datasets/que-faire-a-paris-/records"
    params = {
        "limit": 100,
        "offset": 0
    }   

    logger.info("Fetching Paris events data...")
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    data = response.json()
    records = []
    
    for record in data.get('records', []):
        records.append(record.get('record', {}).get('fields', {}))
    
    if records: 
        # Save as JSON
        json_filename = os.path.join("events_data", f"paris_events.json")
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(records, f, indent=2, ensure_ascii=False)
        logger.info(f"Paris events data saved to {json_filename} (JSON format)")
        
        return json_filename
    else:
        logger.warning("No Paris events data found")
        return None
    
def fetch_rome_events():
    """Fetch events data for Rome (JSON format)"""
    url = "https://serpapi.com/search.json?engine=google_events"
    

    params = {
      "engine": "google_events",
      "q": "Events in Rome",
      "htichips": "date:week",
      "api_key": "daaeefdfd578f239dca6397a32cae057b67b1004eaaef8baa74d9bed0f912c67"
    }

    search = GoogleSearch(params)
    logger.info("Fetching Rome events data...")
    results = search.get_dict()
    events_results = results["events_results"]    
        
    # Save as JSON
    json_filename = os.path.join("events_data", f"rome_events.json")
    with open(json_filename, 'w', encoding='utf-8') as f:
        json.dump(events_results, f, indent=2, ensure_ascii=False)
    logger.info(f"Rome events data saved to {json_filename} (JSON format)")
    
    
    return json_filename


def fetch_madrid_events():
    """Fetch events data for Madrid (CSV and JSON format)"""
    url = "https://datos.madrid.es/egob/catalogo/300107-0-agenda-actividades-eventos.json"
    
    try:
        logger.info("Fetching Madrid events data...")
        response = requests.get(url)
        response.raise_for_status()
        
        # Try to parse JSON, if it fails, attempt to clean the response
        try:
            data = response.json()
        except json.JSONDecodeError as json_error:
            logger.warning(f"JSON decode error: {str(json_error)}")
            # Attempt to clean the response
            cleaned_response = response.text.replace('\\', '\\\\')
            data = json.loads(cleaned_response)
        
        # Madrid API returns data in '@graph' array
        records = data.get('@graph', [])
        
        if records:
            # Save as JSON
            json_filename = os.path.join("events_data", f"madrid_events.json")
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump(records, f, indent=2, ensure_ascii=False)
            logger.info(f"Madrid events data saved to {json_filename} (JSON format)")
            
            return json_filename
        else:
            logger.warning("No Madrid events data found")
            return None
    except Exception as e:
        logger.error(f"Error fetching Madrid events data: {str(e)}")
        return None

with DAG('fetch_events_api', start_date=datetime(2025, 4, 4),
         description='DAG to download events data via API', tags=['events'],
         schedule='@daily', catchup=False):
    
    # Define the GCS bucket name
    gcs_bucket_name = "bdm-project" 
    
    fetch_barcelona_task = PythonOperator(task_id="fetch_barcelona_events", python_callable=fetch_barcelona_events)
    fetch_paris_task = PythonOperator(task_id="fetch_paris_events", python_callable=fetch_paris_events)
    fetch_rome_task = PythonOperator(task_id="fetch_rome_events", python_callable=fetch_rome_events)
    fetch_madrid_task = PythonOperator(task_id="fetch_madrid_events", python_callable=fetch_madrid_events)

     # Create GCS upload tasks for each city
    upload_barcelona_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_barcelona_json',
        src='{{ ti.xcom_pull(task_ids="fetch_barcelona_events") }}',
        dst='tmp_landing_zone/events/{{ execution_date.strftime("%Y_%m_%d").lower() }}_barcelona_api_events.json',
        bucket=gcs_bucket_name,
        gcp_conn_id="google_cloud_default"
    )

    upload_paris_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_paris_json',
        src='{{ ti.xcom_pull(task_ids="fetch_paris_events") }}',
        dst='tmp_landing_zone/events/{{ execution_date.strftime("%Y_%m_%d").lower() }}_paris_api_events.json',
        bucket=gcs_bucket_name,
        gcp_conn_id="google_cloud_default"
    )

    upload_rome_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_rome_json',
        src='{{ ti.xcom_pull(task_ids="fetch_rome_events") }}',
        dst='tmp_landing_zone/events/{{ execution_date.strftime("%Y_%m_%d").lower() }}_rome_api_events.json',
        bucket=gcs_bucket_name,
        gcp_conn_id="google_cloud_default"
    )

    upload_madrid_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_madrid_json',
        src='{{ ti.xcom_pull(task_ids="fetch_madrid_events") }}',
        dst='tmp_landing_zone/events/{{ execution_date.strftime("%Y_%m_%d").lower() }}_madrid_api_events.json',
        bucket=gcs_bucket_name,
        gcp_conn_id="google_cloud_default"
    )

    fetch_barcelona_task >> upload_barcelona_to_gcs
    fetch_paris_task >> upload_paris_to_gcs
    fetch_rome_task >> upload_rome_to_gcs
    fetch_madrid_task >> upload_madrid_to_gcs
