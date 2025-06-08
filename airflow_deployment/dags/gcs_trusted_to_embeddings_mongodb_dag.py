from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import logging
import io
import pytz
import pandas as pd
from sentence_transformers import SentenceTransformer
from pinecone import Pinecone, ServerlessSpec
import uuid
from pymongo import MongoClient
import google.generativeai as genai
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get credentials from environment variables
PINECONE_API_KEY = os.getenv('PINECONE_API_KEY')
MONGODB_CONNECTION_STRING = os.getenv('MONGODB_CONNECTION_STRING')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')

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

def process_events_to_embeddings_mongodb(bucket_name, **context):
    """
    Process events from trusted zone, create embeddings, and store in MongoDB
    """
    # Initialize GCSHook
    gcs_hook = GCSHook()
    storage_client = gcs_hook.get_conn()
    bucket = storage_client.bucket(bucket_name)
    
    # Initialize logging handler
    gcs_handler = GCSLogHandler(bucket_name, gcs_hook, context['dag'].dag_id)
    logger.addHandler(gcs_handler)
    
    try:
        # Initialize embedding model
        model = SentenceTransformer("all-MiniLM-L6-v2")
        
        # Initialize Pinecone
        pc = Pinecone(api_key=PINECONE_API_KEY)
        index_name = "itinerary-events"
        
        # Initialize or connect to Pinecone index
        if index_name not in pc.list_indexes().names():
            pc.create_index(
                name=index_name,
                dimension=384,  # Dimension for all-MiniLM-L6-v2
                metric="cosine",
                spec=ServerlessSpec(
                    cloud='aws',
                    region='us-east-1'
                )
            )
        pinecone_index = pc.Index(index_name)
        
        # Initialize MongoDB connection
        mongo_client = MongoClient(MONGODB_CONNECTION_STRING)
        db = mongo_client["itinerary_db"]
        collection = db["itineraries"]
        
        # Get today's date for file filtering
        today = datetime.now().strftime("%Y_%m_%d")
        
        # Process each city's data
        cities = ['barcelona', 'madrid', 'paris']
        for city in cities:
            logger.info(f"Processing events for {city}")
            
            # List all CSV files for the city
            prefix = f"trusted_zone/events/{city}/{today}/"
            blobs = bucket.list_blobs(prefix=prefix)
            csv_files = [blob for blob in blobs if blob.name.endswith('.csv')]
            
            if not csv_files:
                logger.info(f"No data found for {city} on {today}")
                continue
                
            logger.info(f"Found {len(csv_files)} files to process for {city}")
            
            # Process each CSV file
            for blob in csv_files:
                try:
                    # Download file locally
                    local_file = f"/tmp/{os.path.basename(blob.name)}"
                    blob.download_to_filename(local_file)
                    
                    # Read CSV file
                    df = pd.read_csv(local_file)
                    logger.info(f"Loaded {len(df)} events from {blob.name}")
                    
                    # Create text representations and embeddings
                    vectors = []
                    for _, row in df.iterrows():
                        # Create text representation
                        event_text = f"Event: {row.get('name', 'N/A')}\n"
                        event_text += f"Description: {row.get('description', 'N/A')}\n"
                        event_text += f"Activity: {row.get('activity', 'N/A')}\n"
                        event_text += f"Category: {row.get('category', 'N/A')}\n"
                        event_text += f"Venue: {row.get('venue', 'N/A')}\n"
                        event_text += f"Address: {row.get('address', 'N/A')}\n"
                        event_text += f"Start Date: {row.get('start_date', 'N/A')}\n"
                        event_text += f"Time: {row.get('time', 'N/A')}\n"
                        event_text += f"Price: {'Free' if row.get('is_free') == 'free' else row.get('price_details', 'N/A')}\n"
                        
                        # Create embedding
                        embedding = model.encode(event_text)
                        
                        # Prepare metadata
                        metadata = {
                            'city': city,
                            'event_name': row.get('name', ''),
                            'description': row.get('description', ''),
                            'activity': row.get('activity', ''),
                            'category': row.get('category', ''),
                            'venue': row.get('venue', ''),
                            'address': row.get('address', ''),
                            'start_date': row.get('start_date', ''),
                            'time': row.get('time', ''),
                            'is_free': row.get('is_free', ''),
                            'price_details': row.get('price_details', ''),
                            'website': row.get('website', ''),
                            'audience': row.get('audience', '')
                        }
                        
                        # Remove None values from metadata
                        metadata = {k: v for k, v in metadata.items() if v is not None}
                        
                        # Add to vectors list
                        vectors.append((str(uuid.uuid4()), embedding.tolist(), metadata))
                        
                        # Store in MongoDB
                        document = {
                            'city': city,
                            'type': 'event',
                            'event_data': metadata
                        }
                        collection.insert_one(document)
                    
                    # Upload vectors to Pinecone
                    if vectors:
                        pinecone_index.upsert(vectors=vectors)
                        logger.info(f"Successfully uploaded {len(vectors)} vectors to Pinecone for {city}")
                    
                    # Clean up
                    os.remove(local_file)
                    
                except Exception as e:
                    logger.error(f"Error processing {blob.name}: {str(e)}")
                    continue
        
        # Create MongoDB indexes
        collection.create_index([
            ('city', 1),
            ('type', 1)
        ])
        logger.info("Created MongoDB indexes")
        
    except Exception as e:
        logger.error(f"Error in process_events_to_embeddings_mongodb: {str(e)}")
        raise
    finally:
        # Save logs to GCS
        gcs_handler.save_logs(context)

def run_rag_process(bucket_name, **context):
    """
    Run the RAG process to generate itineraries
    """
    # Initialize GCSHook
    gcs_hook = GCSHook()
    storage_client = gcs_hook.get_conn()
    bucket = storage_client.bucket(bucket_name)
    
    # Initialize logging handler
    gcs_handler = GCSLogHandler(bucket_name, gcs_hook, context['dag'].dag_id)
    logger.addHandler(gcs_handler)
    
    try:
        # Initialize embedding model
        model = SentenceTransformer("all-MiniLM-L6-v2")
        
        # Initialize Gemini
        genai.configure(api_key=GEMINI_API_KEY)
        gemini_model = genai.GenerativeModel('gemini-2.0-flash')
        
        # Initialize Pinecone
        pc = Pinecone(api_key=PINECONE_API_KEY)
        index_name = "itinerary-events"
        pinecone_index = pc.Index(index_name)
        
        # Initialize MongoDB connection
        mongo_client = MongoClient(MONGODB_CONNECTION_STRING)
        db = mongo_client["itinerary_db"]
        collection = db["itineraries"]
        
        # Load existing itineraries from MongoDB
        llm_responses = {}
        itineraries = collection.find({})
        for itinerary in itineraries:
            city = itinerary.get('city')
            category = itinerary.get('type')
            if city and category:
                if city not in llm_responses:
                    llm_responses[city] = {}
                llm_responses[city][category] = itinerary
        
        logger.info(f"Loaded {len(llm_responses)} cities with their itineraries")
        
        # Process each city
        cities = ['barcelona', 'madrid', 'paris']
        categories = ["City Sight Seeing", "Beaches", "Historical Places"]
        
        for city in cities:
            logger.info(f"Generating itinerary for {city}")
            
            # Get relevant context from MongoDB for each category
            contexts = []
            for category in categories:
                if city in llm_responses and category in llm_responses[city]:
                    context = llm_responses[city][category]
                    context_dict = {
                        'city': context.get('city'),
                        'type': context.get('type'),
                        'days': context.get('days'),
                        'itinerary': context.get('itinerary')
                    }
                    contexts.append(context_dict)
            
            # Retrieve relevant events for each category
            all_relevant_events = []
            for category in categories:
                query = f"{category} activities in {city}"
                query_embedding = model.encode([query])[0]
                
                # Search in Pinecone
                results = pinecone_index.query(
                    vector=query_embedding.tolist(),
                    top_k=10,
                    include_metadata=True
                )
                
                # Filter results for the specified city
                for match in results.matches:
                    if match.metadata['city'] == city:
                        all_relevant_events.append(match.metadata)
            
            # Prepare prompt for Gemini
            prompt = f"""You are a travel itinerary expert who creates detailed and practical travel plans.

                        Generate a 3-day itinerary for {city} combining the following categories: {', '.join(categories)}.

                        Previous successful itineraries for reference:
                        {json.dumps(contexts, indent=2)}

                        Available events:
                        {json.dumps(all_relevant_events, indent=2)}

                        Please create a detailed itinerary in the following JSON format:
                        {{
                            "itinerary": {{
                                "city": "{city}",
                                "categories": {json.dumps(categories)},
                                "days": 3,
                                "daily_plans": [
                                    {{
                                        "day": 1,
                                        "activities": [
                                            {{
                                                "time": "HH:MM",
                                                "event_name": "string",
                                                "venue": "string",
                                                "description": "string",
                                                "duration": "string",
                                                "price": "string",
                                                "booking_info": "string",
                                                "category": "string"
                                            }}
                                        ]
                                    }}
                                ],
                                "total_cost": "string",
                                "additional_tips": ["string"]
                            }}
                        }}"""
            
            # Generate itinerary using Gemini
            response = gemini_model.generate_content(prompt)
            
            try:
                # Clean the response text
                response_text = response.text
                if response_text.startswith('```json'):
                    response_text = response_text[7:]
                if response_text.endswith('```'):
                    response_text = response_text[:-3]
                response_text = response_text.strip()
                
                # Parse the response as JSON
                itinerary_json = json.loads(response_text)
                
                # Store the generated itinerary in MongoDB
                document = {
                    'city': city,
                    'types': categories,
                    'days': 3,
                    'itinerary': itinerary_json
                }
                collection.insert_one(document)
                
                logger.info(f"Successfully generated and stored itinerary for {city}")
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse Gemini response as JSON for {city}: {str(e)}")
                continue
        
        # Create MongoDB indexes
        collection.create_index([
            ('city', 1),
            ('types', 1),
            ('days', 1)
        ])
        logger.info("Created MongoDB indexes")
        
    except Exception as e:
        logger.error(f"Error in run_rag_process: {str(e)}")
        raise
    finally:
        # Save logs to GCS
        gcs_handler.save_logs(context)

with DAG(
    'gcs_trusted_to_embeddings_mongodb',
    start_date=datetime(2025, 5, 11),
    description='DAG to process events from trusted zone, create embeddings, and store in MongoDB.',
    tags=['gcs', 'embeddings', 'mongodb', 'events'],
    schedule='@daily',
    catchup=False
) as dag:
    
    process_events_task = PythonOperator(
        task_id='process_events_to_embeddings_mongodb',
        python_callable=process_events_to_embeddings_mongodb,
        op_kwargs={
            'bucket_name': GCS_BUCKET_NAME
        }
    )
    
    run_rag_task = PythonOperator(
        task_id='run_rag_process',
        python_callable=run_rag_process,
        op_kwargs={
            'bucket_name': GCS_BUCKET_NAME
        }
    )
    
    # Set task dependencies
    process_events_task >> run_rag_task 