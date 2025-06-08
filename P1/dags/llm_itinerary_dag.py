import google.generativeai as genai
import os
import time
import logging

from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

api_key = 'AIzaSyAwp27kfYmMpuTjFx2vdG3Lngjpx8Qo4NU'

genai.configure(api_key=api_key)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def generate_content(prompt, model_name="gemini-1.5-pro"):
    model = genai.GenerativeModel(model_name)
    response = model.generate_content(prompt)
    
    return response


def generate_travel_itinerary(destination: str, days: int, interests: str):
    
    # Validate inputs
    valid_interests = [
        "Beaches", "City Sight Seeing", "Outdoor Adventures", 
        "Events", "Food Exploration", "Shopping", "Nightlife"
    ]
    
    if interests not in valid_interests:
        raise ValueError(f"Interest must be one of: {', '.join(valid_interests)}")
    
    if days < 1:
        raise ValueError("Number of days must be at least 1")
    
    # Craft the prompt
    prompt = f"""
    Create a detailed {days}-day itinerary for a trip to {destination} focused on {interests}.
    Include specific attractions, activities, restaurants, and experiences for each day.
    Consider the logical flow of activities throughout each day.
    Format the response as a JSON array where each object has these keys:
    - "location": specific location name within {destination}
    - "time": time of day (morning/afternoon/evening or specific hours)
    - "text": detailed description of the activity or experience
    
    Make sure the itinerary:
    1. Includes {interests} as the primary focus
    2. Has variety while maintaining the theme
    3. Accounts for travel time between locations
    4. Includes at least 3-5 activities per day
    5. Suggests specific restaurants or food experiences
    6. Create a realistic itineary and try to validate the location using search functionality
    """
    
    # Function to call an LLM API
    # This is a placeholder - replace with your actual API implementation
    try:
    
        response = generate_content(prompt) 
        # Parse the JSON response
        # itinerary = json.loads(response.text.strip("```json"))
        
        # # Validate the format
        # for item in itinerary:
        #     if not all(key in item for key in ["location", "time", "text"]):
        #         raise ValueError("API response missing required fields in the JSON structure")
        
        return response.text
        
    except Exception as e:
        print(f"Error generating itinerary: {e}")
        return ''
    

def generate_all_itineraries():

    # cities = ["paris","barcelona","rome","madrid"]
    # valid_interests = [
    #     "Beaches", "City Sight Seeing", "Outdoor Adventures", 
    #     "Events", "Food Exploration", "Shopping", "Nightlife"
    # ]

    cities = ["paris"]
    valid_interests = [ 
        "Food Exploration"
    ]

    os.makedirs("data/llm_responses", exist_ok=True)

    for city in cities:
        for interest in valid_interests:
            for days in range(1,4):
                logger.info(f"Generating itinerary for {city} - {days} days - {interest}")
                itineary = generate_travel_itinerary(city,days,interest)
                time.sleep(30)
     
                filename = f"data/llm_responses/{datetime.now().strftime('%Y_%m_%d').lower()}_itinerary_{city}_{days}_{interest.replace(' ', '_')}.txt"
                with open(filename,"w") as f:
                    f.write(itineary)
                logger.info(f"Itinerary saved to {filename}")

    
with DAG('generate_llm_itineraries', start_date=datetime(2025, 4, 4),
         description='Generate travel itineraries using Gemini', schedule_interval='@monthly',
         tags=['itinerary'], catchup= False) as dag:

    # Define the GCS bucket name
    gcs_bucket_name = "bdm-project"

    run_generation_task = PythonOperator(
        task_id='generate_all_itineraries',
        python_callable=generate_all_itineraries
    )
    
    upload_itineraries_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_itineraries_to_gcs',
        src='data/llm_responses/*',
        dst='tmp_landing_zone/llm/',
        bucket=gcs_bucket_name,  
        gcp_conn_id="google_cloud_default"
    )

    run_generation_task >> upload_itineraries_to_gcs