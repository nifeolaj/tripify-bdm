# app.py
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import os
from dotenv import load_dotenv
from backend.rag import ItineraryRAG
from backend.enhanced_itinerary import EnhancedItineraryGenerator
from backend.database import db
from datetime import datetime
import logging
import asyncio
from backend.data_collector import DataCollector
import uuid
import json
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from backend.transport_scraper import TransportScraper
from backend.transport_consumer import TransportConsumer
from backend.routes import getStaticTransportOptions
import time
import atexit

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

app = Flask(__name__, 
    static_folder='static',
    template_folder='templates'
)
CORS(app)

# Initialize RAG system
logger.info("Initializing RAG system...")
try:
    rag_system = ItineraryRAG(
        gemini_api_key=os.getenv('GEMINI_API_KEY'),
        pinecone_api_key=os.getenv('PINECONE_API_KEY'),
        pinecone_environment=os.getenv('PINECONE_ENVIRONMENT'),
        mongo_uri=os.getenv('MONGO_URI')
    )
    logger.info("RAG system initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize RAG system: {str(e)}", exc_info=True)
    raise

# Initialize Enhanced Itinerary Generator
logger.info("Initializing Enhanced Itinerary Generator...")
try:
    enhanced_generator = EnhancedItineraryGenerator(
        rag_system=rag_system,
        graph_api_url=None  # We'll use local Neo4j directly
    )
    logger.info("Enhanced Itinerary Generator initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Enhanced Itinerary Generator: {str(e)}", exc_info=True)
    raise

# Load existing itineraries
logger.info("Loading existing itineraries...")
try:
    rag_system.load_llm_responses()
    logger.info("Existing itineraries loaded successfully")
except Exception as e:
    logger.error(f"Failed to load existing itineraries: {str(e)}", exc_info=True)
    raise

# Initialize data collector
data_collector = DataCollector()

# Initialize scheduler
scheduler = BackgroundScheduler()

# Add these after other initializations
transport_scraper = TransportScraper()
transport_consumer = TransportConsumer()
transport_consumer.start()

def schedule_daily_flush():
    """Schedule the daily data flush"""
    # Schedule job to run at 23:59 every day
    scheduler.add_job(
        func=data_collector.flush_all,
        trigger=CronTrigger(hour=23, minute=59),
        id='daily_flush',
        name='Flush all collected data to GCS',
        replace_existing=True
    )
    scheduler.start()

@app.route('/')
def home():
    return render_template('main.html')

@app.route('/booking')
def booking():
    return render_template('booking.html')

@app.route('/trip')
def trip():
    return render_template('Trip.html')

@app.route('/itineraries')
def itineraries():
    return render_template('itineraries.html')

@app.route('/api/v1/cities/popular', methods=['GET'])
def get_popular_cities():
    """Get list of all available cities"""
    try:
        with db.get_session() as session:
            query = """
            MATCH (c:City)
            OPTIONAL MATCH (p:Post)-[:IN]->(c)
            WITH c, count(DISTINCT p) as post_count
            RETURN c,
                   post_count as popularity
            ORDER BY post_count DESC
            """
            result = session.run(query)
            cities = []
            for record in result:
                city = dict(record["c"])
                city["popularity"] = record["popularity"]
                cities.append(city)
            return jsonify(cities)
    except Exception as e:
        logger.error(f"Error fetching cities: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/v1/itineraries/recommended', methods=['GET'])
def get_recommended_itineraries():
    """Get personalized itinerary recommendations based on user preferences"""
    try:
        user_id = request.args.get('user_id')
        
        with db.get_session() as session:
            # If no user_id provided, use a default user
            if not user_id:
                # First check if default user exists
                default_user_query = """
                MATCH (u:User {id: 'default_user'})
                RETURN u
                """
                default_user = session.run(default_user_query).single()
                
                if not default_user:
                    # Create default user if doesn't exist
                    create_default_user_query = """
                    CREATE (u:User {
                        id: 'default_user',
                        user_id: 'default_user',
                        name: 'Default User',
                        username: 'default_user',
                        join_date: datetime()
                    })
                    RETURN u
                    """
                    session.run(create_default_user_query)
                
                user_id = 'default_user'

            # Get user's preferred cities from their preferences
            preferences_query = """
            MATCH (u:User {id: $user_id})-[:FAVORITE]->(c:City)
            RETURN collect(c.id) as preferred_cities
            """
            preferences_result = session.run(preferences_query, user_id=user_id)
            preferences_record = preferences_result.single()
            preferred_cities = preferences_record["preferred_cities"] if preferences_record else []

            if not preferred_cities:
                return jsonify([])

            # First get posts that the user has liked
            liked_posts_query = """
            MATCH (u:User {id: $user_id})-[l:LIKED]->(p:Post)-[:IN]->(city:City)
            WHERE city.id IN $preferred_cities
            MATCH (p)-[:INCLUDES]->(a:Activity)
            WITH p, city, collect(a) as activities, true as is_liked, 1 as priority
            RETURN p, city, activities, is_liked, priority
            """
            
            # Then get posts from creators of liked posts
            creator_posts_query = """
            MATCH (u:User {id: $user_id})-[l:LIKED]->(p1:Post)<-[:CREATED]-(creator:User)
            MATCH (creator)-[:CREATED]->(p2:Post)-[:IN]->(city:City)
            WHERE city.id IN $preferred_cities
            AND p2.id <> p1.id
            MATCH (p2)-[:INCLUDES]->(a:Activity)
            WITH p2 as p, city, collect(a) as activities, false as is_liked, 2 as priority
            RETURN p, city, activities, is_liked, priority
            """
            
            # Finally get other posts in preferred cities
            other_posts_query = """
            MATCH (p:Post)-[:IN]->(city:City)
            WHERE city.id IN $preferred_cities
            AND NOT EXISTS((:User {id: $user_id})-[:LIKED]->(p))
            AND NOT EXISTS((:User {id: $user_id})-[:LIKED]->(:Post)<-[:CREATED]-(:User)-[:CREATED]->(p))
            MATCH (p)-[:INCLUDES]->(a:Activity)
            WITH p, city, collect(a) as activities, false as is_liked, 3 as priority
            RETURN p, city, activities, is_liked, priority
            """
            
            # Combine all results
            combined_query = """
            CALL {
                """ + liked_posts_query + """
                UNION
                """ + creator_posts_query + """
                UNION
                """ + other_posts_query + """
            }
            RETURN p, city, activities, is_liked
            ORDER BY priority, p.created_at DESC
            LIMIT 20
            """
            
            result = session.run(
                combined_query,
                preferred_cities=preferred_cities,
                user_id=user_id
            )
            
            itineraries = []
            for record in result:
                itinerary = {
                    "post": dict(record["p"]),
                    "city": dict(record["city"]),
                    "activities": [dict(activity) for activity in record["activities"]],
                    "is_liked": record["is_liked"]
                }
                itineraries.append(itinerary)
            
            return jsonify(itineraries)
    except Exception as e:
        logger.error(f"Error fetching recommended itineraries: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/v1/users/current/preferences', methods=['GET'])
def get_current_user_preferences():
    """Get preferences for the current user"""
    try:
        with db.get_session() as session:
            # First check if default user exists
            check_user_query = """
            MATCH (u:User {id: 'default_user'})
            RETURN u
            """
            default_user = session.run(check_user_query).single()
            
            if not default_user:
                # Create default user if doesn't exist
                create_user_query = """
                CREATE (u:User {
                    id: 'default_user',
                    name: 'Default User',
                    username: 'default_user',
                    join_date: datetime()
                })
                RETURN u
                """
                default_user = session.run(create_user_query).single()
                
                # Collect user data for new user
                user_data = {
                    'user_id': 'default_user',
                    'name': 'Default User',
                    'email': 'default@example.com',
                    'username': 'default_user',
                    'bio': 'Default user account',
                    'join_date': datetime.now().isoformat(),
                    'location': 'Unknown',
                    'profile_picture': 'https://picsum.photos/200',
                    'favorite_cities': []
                }
                data_collector.collect_user(user_data)
            
            # Get user's favorite cities
            query = """
            MATCH (u:User {id: 'default_user'})
            OPTIONAL MATCH (u)-[:FAVORITE]->(c:City)
            RETURN collect(c.id) as favorite_cities
            """
            result = session.run(query)
            record = result.single()
            
            if record:
                preferences = {
                    "favorite_cities": record["favorite_cities"] or [],
                    "travel_style": "cultural"  # Default value
                }
                return jsonify(preferences)
            return jsonify({"favorite_cities": [], "travel_style": "cultural"})
    except Exception as e:
        logger.error(f"Error fetching user preferences: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/v1/users/current/preferences', methods=['POST'])
def update_current_user_preferences():
    """Update preferences for the current user"""
    try:
        preferences = request.json
        with db.get_session() as session:
            # First check if default user exists
            check_user_query = """
            MATCH (u:User {id: 'default_user'})
            RETURN u
            """
            default_user = session.run(check_user_query).single()
            
            if not default_user:
                # Create default user if doesn't exist
                create_user_query = """
                CREATE (u:User {
                    id: 'default_user',
                    name: 'Default User',
                    username: 'default_user',
                    join_date: datetime()
                })
                RETURN u
                """
                default_user = session.run(create_user_query).single()
                
                # Collect user data for new user
                user_data = {
                    'user_id': 'default_user',
                    'name': 'Default User',
                    'email': 'default@example.com',
                    'username': 'default_user',
                    'bio': 'Default user account',
                    'join_date': datetime.now().isoformat(),
                    'location': 'Unknown',
                    'profile_picture': 'https://picsum.photos/200',
                    'favorite_cities': preferences.get('favorite_cities', [])
                }
                data_collector.collect_user(user_data)
            else:
                # Update existing user data
                user_data = {
                    'user_id': 'default_user',
                    'name': 'Default User',
                    'email': 'default@example.com',
                    'username': 'default_user',
                    'bio': 'Default user account',
                    'join_date': default_user['u']['join_date'].isoformat(),
                    'location': 'Unknown',
                    'profile_picture': 'https://picsum.photos/200',
                    'favorite_cities': preferences.get('favorite_cities', [])
                }
                data_collector.collect_user(user_data)

            # First, remove all existing FAVORITE relationships
            delete_query = """
            MATCH (u:User {id: 'default_user'})-[f:FAVORITE]->(c:City)
            DELETE f
            """
            session.run(delete_query)

            # Then create new FAVORITE relationships for selected cities
            if preferences.get('favorite_cities'):
                create_query = """
                MATCH (u:User {id: 'default_user'})
                WITH u
                UNWIND $favorite_cities as city_id
                MATCH (c:City {id: city_id})
                MERGE (u)-[:FAVORITE {created_at: datetime()}]->(c)
                """
                session.run(create_query, favorite_cities=preferences.get('favorite_cities', []))

            # Get updated preferences
            get_query = """
            MATCH (u:User {id: 'default_user'})
            OPTIONAL MATCH (u)-[:FAVORITE]->(c:City)
            RETURN collect(c.id) as favorite_cities
            """
            result = session.run(get_query)
            record = result.single()
            
            if record:
                updated_preferences = {
                    "favorite_cities": record["favorite_cities"] or [],
                    "budget": preferences.get('budget', 2000)  # Default budget if not provided
                }
                return jsonify(updated_preferences)
            return jsonify({"favorite_cities": [], "budget": 2000})
    except Exception as e:
        logger.error(f"Error updating user preferences: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/v1/generate-itinerary', methods=['POST'])
async def generate_itinerary():
    try:
        data = request.json
        logger.info(f"Received request data: {data}")
        
        destinations = data.get('destinations', [])
        dates = data.get('dates', {})
        preferences = data.get('preferences', {})
        budget = data.get('budget', '500')
        user_id = data.get('user_id')

        if not destinations:
            logger.error("No destinations provided in request")
            return jsonify({"error": "No destinations provided"}), 400

        # Get the main destination (last city in the list)
        main_destination = destinations[-1]
        
        # Extract categories from preferences
        categories = []
        if preferences.get('interests'):
            categories.extend(preferences['interests'])
        
        # Calculate number of days from dates
        days = 1
        if dates and 'start' in dates and 'end' in dates:
            start_date = datetime.strptime(dates['start'], '%Y-%m-%d')
            end_date = datetime.strptime(dates['end'], '%Y-%m-%d')
            days = (end_date - start_date).days + 1

        # Generate enhanced itinerary
        itinerary = await enhanced_generator.generate_enhanced_itinerary(
            city=main_destination,
            categories=categories,
            days=days,
            user_id=user_id
        )

        # Collect post data
        post_data = {
            'post_id': str(uuid.uuid4()),
            'user_id': user_id or 'default_user',
            'city': main_destination,
            'title': f"{days}-Day {main_destination} Itinerary",
            'description': itinerary.get('description', ''),
            'start_date': dates.get('start', datetime.now().isoformat()),
            'end_date': dates.get('end', datetime.now().isoformat()),
            'daily_plans': json.dumps(itinerary.get('daily_plans', [])),
            'total_cost': f"€{budget}",
            'created_at': datetime.now().isoformat(),
            'likes_count': 0,
            'comments_count': 0
        }
        data_collector.collect_post(post_data)

        return jsonify(itinerary)
    except Exception as e:
        logger.error(f"Error generating itinerary: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/v1/itineraries/<itinerary_id>/like', methods=['POST'])
def like_itinerary(itinerary_id):
    """Like or unlike an itinerary"""
    try:
        with db.get_session() as session:
            # First check if default user exists
            check_user_query = """
            MATCH (u:User {id: 'default_user'})
            RETURN u
            """
            default_user = session.run(check_user_query).single()
            
            if not default_user:
                # Create default user if doesn't exist
                create_user_query = """
                CREATE (u:User {
                    id: 'default_user',
                    name: 'Default User',
                    username: 'default_user',
                    join_date: datetime()
                })
                RETURN u
                """
                session.run(create_user_query)

            # Check if the like relationship exists
            check_like_query = """
            MATCH (u:User {id: 'default_user'})-[l:LIKED]->(p:Post {id: $post_id})
            RETURN l
            """
            like_exists = session.run(check_like_query, post_id=itinerary_id).single()

            if like_exists:
                # Unlike: Remove the relationship and decrease vote count
                unlike_query = """
                MATCH (u:User {id: 'default_user'})-[l:LIKED]->(p:Post {id: $post_id})
                DELETE l
                WITH p
                SET p.votes = p.votes - 1
                RETURN p
                """
                result = session.run(unlike_query, post_id=itinerary_id).single()
                liked = False
            else:
                # Like: Create the relationship and increase vote count
                like_query = """
                MATCH (u:User {id: 'default_user'})
                MATCH (p:Post {id: $post_id})
                MERGE (u)-[l:LIKED {created_at: datetime()}]->(p)
                WITH p
                SET p.votes = COALESCE(p.votes, 0) + 1
                RETURN p
                """
                result = session.run(like_query, post_id=itinerary_id).single()
                liked = True

                # Collect like data
                data_collector.collect_like('default_user', itinerary_id)

            if result:
                return jsonify({
                    "votes": result["p"].get("votes", 0),
                    "liked": liked
                })
            return jsonify({"error": "Itinerary not found"}), 404

    except Exception as e:
        logger.error(f"Error liking/unliking itinerary: {str(e)}")
        return jsonify({"error": str(e)}), 500

# Add a new endpoint to flush data to GCS
@app.route('/api/v1/flush-data', methods=['POST'])
def flush_data():
    """Flush all collected data to GCS"""
    try:
        data_collector.flush_all()
        return jsonify({"message": "Data flushed successfully"}), 200
    except Exception as e:
        logger.error(f"Error flushing data: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/v1/data-collector/status', methods=['GET'])
def check_data_collector_status():
    """Check the status of data collector buffers"""
    try:
        status = {
            'likes_buffer_size': len(data_collector.likes_buffer),
            'posts_buffer_size': len(data_collector.posts_buffer),
            'users_buffer_size': len(data_collector.users_buffer),
            'last_flush_time': data_collector.last_flush_time.isoformat() if hasattr(data_collector, 'last_flush_time') else None,
            'buffer_limit': data_collector.buffer_limit
        }
        return jsonify(status)
    except Exception as e:
        logger.error(f"Error checking data collector status: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/v1/data-collector/test', methods=['POST'])
def test_data_collector():
    """Test the data collector by adding sample data"""
    try:
        # Test like collection
        data_collector.collect_like('test_user', 'test_post')
        
        # Test post collection
        test_post = {
            'post_id': str(uuid.uuid4()),
            'user_id': 'test_user',
            'city': 'Test City',
            'title': 'Test Post',
            'description': 'This is a test post',
            'start_date': datetime.now().isoformat(),
            'end_date': datetime.now().isoformat(),
            'daily_plans': json.dumps([{'day': 1, 'activities': []}]),
            'total_cost': '€100',
            'created_at': datetime.now().isoformat(),
            'likes_count': 0,
            'comments_count': 0
        }
        data_collector.collect_post(test_post)
        
        # Test user collection
        test_user = {
            'user_id': 'test_user',
            'name': 'Test User',
            'email': 'test@example.com',
            'username': 'test_user',
            'bio': 'Test bio',
            'join_date': datetime.now().isoformat(),
            'location': 'Test Location',
            'profile_picture': 'https://picsum.photos/200',
            'favorite_cities': ['Test City']
        }
        data_collector.collect_user(test_user)
        
        return jsonify({
            "message": "Test data added successfully",
            "status": {
                'likes_buffer_size': len(data_collector.likes_buffer),
                'posts_buffer_size': len(data_collector.posts_buffer),
                'users_buffer_size': len(data_collector.users_buffer)
            }
        })
    except Exception as e:
        logger.error(f"Error testing data collector: {str(e)}")
        return jsonify({"error": str(e)}), 500

# Add this new endpoint
@app.route('/api/v1/transport-options', methods=['GET'])
def get_transport_options():
    """Get transport options between cities"""
    try:
        from_city = request.args.get('from')
        to_city = request.args.get('to')
        date = request.args.get('date')
        
        if not all([from_city, to_city, date]):
            return jsonify({"error": "Missing required parameters"}), 400
            
        # Always return static options
        static_options = getStaticTransportOptions(from_city, to_city)
        return jsonify(static_options)
        
    except Exception as e:
        logger.error(f"Error in get_transport_options: {str(e)}")
        return jsonify({"error": "Failed to get transport options"}), 500

@app.route('/api/v1/accommodation-options', methods=['GET'])
def get_accommodation_options():
    """Get accommodation options for a city"""
    try:
        city = request.args.get('city')
        date = request.args.get('date')
        
        if not all([city, date]):
            return jsonify({"error": "Missing required parameters"}), 400
            
        # Static accommodation options
        static_options = [
            {
                'name': 'Grand Hotel',
                'price': 120,
                'rating': 4.5,
                'type': 'Hotel',
                'image': 'https://images.unsplash.com/photo-1566073771259-6a8506099945?w=500&h=300&fit=crop',
                'amenities': ['Free WiFi', 'Swimming Pool', 'Spa', 'Restaurant']
            },
            {
                'name': 'City Hostel',
                'price': 30,
                'rating': 4.0,
                'type': 'Hostel',
                'image': 'https://images.unsplash.com/photo-1555854877-bab0e564b8d5?w=500&h=300&fit=crop',
                'amenities': ['Free WiFi', 'Shared Kitchen', 'Common Room']
            },
            {
                'name': 'Luxury Apartment',
                'price': 150,
                'rating': 4.8,
                'type': 'Apartment',
                'image': 'https://images.unsplash.com/photo-1522708323590-d24dbb6b0267?w=500&h=300&fit=crop',
                'amenities': ['Free WiFi', 'Kitchen', 'Washing Machine', 'Balcony']
            }
        ]
        
        return jsonify(static_options)
        
    except Exception as e:
        logger.error(f"Error in get_accommodation_options: {str(e)}")
        return jsonify({"error": "Failed to get accommodation options"}), 500

# Add cleanup on shutdown
@atexit.register
def cleanup():
    transport_consumer.stop()

# Add scheduler initialization to the main block
if __name__ == '__main__':
    schedule_daily_flush()
    app.run(debug=True, host='0.0.0.0', port=5001)