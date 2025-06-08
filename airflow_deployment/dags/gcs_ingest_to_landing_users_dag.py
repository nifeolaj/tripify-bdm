from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
from faker import Faker
import random
import json
from pathlib import Path
import io
import pytz
from airflow.models import Variable
import numpy as np
from datetime import timedelta
import logging

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
        log_path = f"landing_zone/logs/{timestamp}.log"
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

class ItineraryDataGenerator:
    def __init__(self):
        """Initialize the data generator with Faker"""
        self.fake = Faker()
        self.cities = ['Paris', 'Rome', 'Barcelona', 'Madrid']
        self.activities = {
            'Paris': [
                'Eiffel Tower Visit', 'Louvre Museum Tour', 'Seine River Cruise',
                'Notre Dame Cathedral', 'Montmartre Walking Tour', 'Versailles Palace',
                'Champs-Élysées Shopping', 'Latin Quarter Food Tour'
            ],
            'Rome': [
                'Colosseum Tour', 'Vatican Museums', 'St. Peter\'s Basilica',
                'Roman Forum', 'Trevi Fountain', 'Spanish Steps',
                'Pantheon Visit', 'Trastevere Food Tour'
            ],
            'Barcelona': [
                'Sagrada Familia', 'Park Güell', 'La Rambla Walk',
                'Gothic Quarter Tour', 'Camp Nou Stadium', 'Barceloneta Beach',
                'Montjuïc Castle', 'Tapas Tour'
            ],
            'Madrid': [
                'Prado Museum', 'Royal Palace', 'Retiro Park',
                'Plaza Mayor', 'Gran Via', 'Puerta del Sol',
                'Mercado de San Miguel', 'Flamenco Show'
            ]
        }
        
        # Initialize data storage
        self.users = []
        self.posts = []
        self.likes = []
        
    def generate_user(self):
        """Generate a fake user profile"""
        return {
            'user_id': str(self.fake.uuid4()),
            'name': self.fake.name(),
            'email': self.fake.email(),
            'username': self.fake.user_name(),
            'bio': self.fake.text(max_nb_chars=200),
            'join_date': self.fake.date_time_between(start_date='-2y', end_date='now').isoformat(),
            'location': self.fake.city(),
            'profile_picture': self.fake.image_url(),
            'favorite_cities': random.sample(self.cities, random.randint(1, 3))
        }
    
    def generate_itinerary_description(self, city, duration, daily_plans):
        """Generate a natural language description of the itinerary"""
        descriptions = [
            f"I recently spent {duration} amazing days in {city} and wanted to share my perfect itinerary! ",
            f"Here's my carefully planned {duration}-day adventure in {city} that I absolutely loved! ",
            f"After multiple visits to {city}, I've crafted this {duration}-day itinerary that covers all the must-see spots! ",
            f"Planning a trip to {city}? Here's my tried and tested {duration}-day itinerary! "
        ]
        
        description = random.choice(descriptions)
        
        # Add highlights from daily plans
        highlights = []
        for day in daily_plans:
            activities = day['activities']
            if activities:
                highlight_activity = random.choice(activities)
                highlights.append(f"Day {day['day']} features {highlight_activity['activity']} at {highlight_activity['time']}")
        
        if highlights:
            description += "Highlights include: " + "; ".join(highlights) + ". "
        
        # Add personal touches
        personal_touches = [
            "I made sure to include a mix of popular attractions and hidden gems.",
            "The timing of activities is optimized to avoid crowds.",
            "I've included some local favorites that tourists often miss.",
            "This itinerary balances cultural experiences with relaxation time.",
            "I've arranged activities to minimize travel time between locations.",
            "The schedule includes both morning and evening activities to make the most of each day."
        ]
        description += random.choice(personal_touches) + " "
        
        # Add practical information
        practical_info = [
            "I recommend booking tickets in advance for major attractions.",
            "Public transportation is the best way to get around.",
            "Most attractions are within walking distance of each other.",
            "Consider getting a city pass to save on entrance fees.",
            "The best time to visit museums is early morning or late afternoon.",
            "Local restaurants are usually less crowded during off-peak hours."
        ]
        description += random.choice(practical_info)
        
        return description
    
    def generate_itinerary_post(self, user_id):
        """Generate a fake itinerary post"""
        city = random.choice(self.cities)
        start_date = self.fake.date_time_between(start_date='-1y', end_date='+1y')
        duration = random.randint(1, 7)
        end_date = start_date + timedelta(days=duration)
        
        # Generate daily activities
        daily_plans = []
        for day in range(duration):
            num_activities = random.randint(2, 4)
            day_activities = []
            
            for _ in range(num_activities):
                activity = random.choice(self.activities[city])
                start_time = f"{random.randint(9, 15):02d}:00"
                duration = random.randint(1, 4)
                
                # Generate detailed activity description
                activity_descriptions = {
                    'Eiffel Tower Visit': 'Experience the iconic symbol of Paris from both ground level and the observation deck.',
                    'Louvre Museum Tour': 'Explore the world\'s largest art museum, home to the Mona Lisa and countless masterpieces.',
                    'Seine River Cruise': 'Enjoy a scenic boat ride along the Seine, passing by major landmarks and beautiful bridges.',
                    'Notre Dame Cathedral': 'Marvel at the stunning Gothic architecture and intricate details of this historic cathedral.',
                    'Montmartre Walking Tour': 'Discover the artistic neighborhood with its charming streets and the Sacré-Cœur Basilica.',
                    'Versailles Palace': 'Tour the opulent palace and its magnificent gardens, once home to French royalty.',
                    'Champs-Élysées Shopping': 'Stroll down the famous avenue, lined with luxury boutiques and charming cafés.',
                    'Latin Quarter Food Tour': 'Sample authentic French cuisine in this historic district known for its restaurants.',
                    'Colosseum Tour': 'Step back in time at the largest amphitheater ever built, learning about gladiatorial games.',
                    'Vatican Museums': 'Admire the extensive collection of art and the breathtaking Sistine Chapel.',
                    'St. Peter\'s Basilica': 'Visit the largest church in the world and climb to the dome for panoramic views.',
                    'Roman Forum': 'Explore the ruins of ancient Rome\'s political and social center.',
                    'Trevi Fountain': 'Toss a coin in the famous fountain and admire its Baroque architecture.',
                    'Spanish Steps': 'Climb the iconic steps and enjoy the view of the city from the top.',
                    'Pantheon Visit': 'Marvel at the perfectly preserved ancient Roman temple and its impressive dome.',
                    'Trastevere Food Tour': 'Experience authentic Roman cuisine in this charming neighborhood.',
                    'Sagrada Familia': 'Admire Gaudi\'s unfinished masterpiece and its unique architectural style.',
                    'Park Güell': 'Explore the colorful park with its mosaic-covered structures and city views.',
                    'La Rambla Walk': 'Stroll down the famous boulevard, experiencing the vibrant street life.',
                    'Gothic Quarter Tour': 'Discover medieval Barcelona through its narrow streets and historic buildings.',
                    'Camp Nou Stadium': 'Visit the home of FC Barcelona and its impressive museum.',
                    'Barceloneta Beach': 'Relax on the city\'s most popular beach and enjoy fresh seafood.',
                    'Montjuïc Castle': 'Explore the historic castle and enjoy panoramic views of the city.',
                    'Tapas Tour': 'Sample various Spanish tapas in local bars and restaurants.',
                    'Prado Museum': 'Discover one of the world\'s finest art collections, featuring Spanish masters.',
                    'Royal Palace': 'Tour the official residence of the Spanish Royal Family.',
                    'Retiro Park': 'Enjoy the beautiful gardens, lake, and crystal palace in this central park.',
                    'Plaza Mayor': 'Experience the historic main square and its surrounding architecture.',
                    'Gran Via': 'Walk along Madrid\'s main shopping street, known for its impressive buildings.',
                    'Puerta del Sol': 'Visit the central square and the famous clock tower.',
                    'Mercado de San Miguel': 'Sample gourmet tapas in this historic covered market.',
                    'Flamenco Show': 'Experience the passion and energy of traditional Spanish dance.'
                }
                
                description = activity_descriptions.get(activity, self.fake.text(max_nb_chars=100))
                
                day_activities.append({
                    'time': start_time,
                    'activity': activity,
                    'duration': f"{duration} hours",
                    'location': self.fake.address(),
                    'description': description
                })
            
            daily_plans.append({
                'day': day + 1,
                'activities': day_activities
            })
        
        # Generate natural language description
        description = self.generate_itinerary_description(city, duration, daily_plans)
        
        return {
            'post_id': str(self.fake.uuid4()),
            'user_id': user_id,
            'city': city,
            'title': f"{duration}-Day {city} Itinerary",
            'description': description,
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'daily_plans': daily_plans,
            'total_cost': f"€{random.randint(500, 3000)}",
            'created_at': self.fake.date_time_between(start_date='-1y', end_date='now').isoformat(),
            'likes_count': 0,
            'comments_count': 0
        }
    
    def generate_like(self, user_id, post_id):
        """Generate a fake like"""
        return {
            'like_id': str(self.fake.uuid4()),
            'user_id': user_id,
            'post_id': post_id,
            'created_at': self.fake.date_time_between(start_date='-1y', end_date='now').isoformat()
        }
    
    def generate_dataset(self, num_users=100, posts_per_user=(1, 5), likes_per_user=(0, 20)):
        """Generate the complete dataset"""
        logger.info("Generating users...")
        for _ in range(num_users):
            self.users.append(self.generate_user())
        
        logger.info("Generating posts...")
        for user in self.users:
            num_posts = random.randint(posts_per_user[0], posts_per_user[1])
            for _ in range(num_posts):
                post = self.generate_itinerary_post(user['user_id'])
                self.posts.append(post)
        
        logger.info("Generating likes...")
        for user in self.users:
            num_likes = random.randint(likes_per_user[0], likes_per_user[1])
            liked_posts = random.sample(self.posts, min(num_likes, len(self.posts)))
            for post in liked_posts:
                like = self.generate_like(user['user_id'], post['post_id'])
                self.likes.append(like)
                post['likes_count'] += 1
        
        logger.info("Dataset generation completed!")
    
    def save_dataset(self, output_dir='data'):
        """Save the generated dataset to CSV files"""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Convert to DataFrames
        users_df = pd.DataFrame(self.users)
        posts_df = pd.DataFrame(self.posts)
        likes_df = pd.DataFrame(self.likes)
        
        # Save to CSV
        users_df.to_csv(output_path / 'users.csv', index=False)
        posts_df.to_csv(output_path / 'posts.csv', index=False)
        likes_df.to_csv(output_path / 'likes.csv', index=False)
        
        logger.info(f"Dataset saved to {output_path}")
        
        # Print some statistics
        logger.info(f"Generated {len(self.users)} users")
        logger.info(f"Generated {len(self.posts)} posts")
        logger.info(f"Generated {len(self.likes)} likes")

def generate_and_upload_dataset(bucket_name, **context):
    """Generate the dataset and upload users.csv, posts.csv, and likes.csv files to the landing_zone in GCS."""
    gcs_hook = GCSHook()
    storage_client = gcs_hook.get_conn()
    bucket = storage_client.bucket(bucket_name)

    gcs_handler = capture_airflow_logs(bucket_name, context['dag'].dag_id, context)
    logger = logging.getLogger(f"airflow.task.{context['task'].task_id}")

    try:
        # Initialize the ItineraryDataGenerator
        generator = ItineraryDataGenerator()

        # Generate the dataset
        generator.generate_dataset(
            num_users=3000,  # Number of users
            posts_per_user=(1, 10),  # Min and max posts per user
            likes_per_user=(0, 10)  # Min and max likes per user
        )

        # Save the dataset to CSV files in the data directory
        output_dir = 'data'
        generator.save_dataset(output_dir=output_dir)

        # Get today's date in the format YYYY-MM-DD
        today = datetime.now().strftime("%Y-%m-%d")

        # Define the landing zone path
        landing_path = f"landing_zone/users/{today}/"

        # Upload users.csv from the data directory
        users_file = os.path.join(output_dir, "users.csv")
        users_blob = bucket.blob(landing_path + "users.csv")
        users_blob.upload_from_filename(users_file)
        logger.info(f"Uploaded users.csv to {landing_path}")

        # Upload posts.csv from the data directory
        posts_file = os.path.join(output_dir, "posts.csv")
        posts_blob = bucket.blob(landing_path + "posts.csv")
        posts_blob.upload_from_filename(posts_file)
        logger.info(f"Uploaded posts.csv to {landing_path}")

        # Upload likes.csv from the data directory
        likes_file = os.path.join(output_dir, "likes.csv")
        likes_blob = bucket.blob(landing_path + "likes.csv")
        likes_blob.upload_from_filename(likes_file)
        logger.info(f"Uploaded likes.csv to {landing_path}")

        # Delete temporary files after uploading
        os.remove(users_file)
        os.remove(posts_file)
        os.remove(likes_file)
        logger.info("Temporary files deleted after upload.")

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    finally:
        gcs_handler.save_logs(context)

with DAG(
    'gcs_ingest_to_landing_users_dag',
    start_date=datetime(2025, 5, 11),
    description='DAG to generate synthetic user data and upload it to GCS landing zone.',
    tags=['gcs', 'users', 'landing'],
    schedule='@daily',
    catchup=False
) as dag:
    gcs_bucket_name = Variable.get("events_gcs_bucket_name", default_var="bdm-project")

    upload_task = PythonOperator(
        task_id='upload_users_to_gcs',
        python_callable=generate_and_upload_dataset,
        op_kwargs={'bucket_name': gcs_bucket_name}
    ) 