import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import folium
from streamlit_folium import folium_static
import calendar
from google.cloud import storage
from google.oauth2 import service_account
import tempfile
import os
import json
from neo4j import GraphDatabase
import networkx as nx
import matplotlib.pyplot as plt
from collections import Counter

# Set page configuration
st.set_page_config(
    page_title="Tripify Events Explorer",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Add custom CSS for styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #3366FF;
        text-align: center;
    }
    .subheader {
        font-size: 1.5rem;
        color: #555;
        margin-bottom: 20px;
    }
    .metric-container {
        background-color: #f8f9fa;
        border-radius: 10px;
        padding: 15px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .filter-section {
        background-color: #f1f3f6;
        padding: 15px;
        border-radius: 10px;
        margin-bottom: 20px;
    }
</style>
""", unsafe_allow_html=True)

# Title and description
st.markdown("<h1 class='main-header'>Tripify Events Explorer</h1>", unsafe_allow_html=True)
st.markdown("<p class='subheader'>Discover events in Barcelona, Madrid, and Paris - perfect for travelers planning their next European adventure!</p>", unsafe_allow_html=True)

# Neo4j Connection
@st.cache_resource
def get_neo4j_connection():
    # Get Neo4j credentials from secrets
    neo4j_creds = st.secrets["neo4j"]
    
    driver = GraphDatabase.driver(
        neo4j_creds["uri"],
        auth=(neo4j_creds["user"], neo4j_creds["password"])
    )
    return driver

# Neo4j Query Functions
@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_popular_cities():
    driver = get_neo4j_connection()
    with driver.session() as session:
        result = session.run("""
            MATCH (c:City)<-[:IN]-(p:Post)
            RETURN c.name as city, count(p) as post_count
            ORDER BY post_count DESC
        """)
        return pd.DataFrame([dict(record) for record in result])

@st.cache_data(ttl=3600)
def get_popular_activities():
    driver = get_neo4j_connection()
    with driver.session() as session:
        result = session.run("""
            MATCH (a:Activity)<-[:INCLUDES]-(p:Post)
            RETURN a.name as activity, count(p) as post_count
            ORDER BY post_count DESC
            LIMIT 10
        """)
        return pd.DataFrame([dict(record) for record in result])

@st.cache_data(ttl=3600)
def get_user_activity_stats():
    driver = get_neo4j_connection()
    with driver.session() as session:
        result = session.run("""
            MATCH (u:User)
            OPTIONAL MATCH (u)-[:CREATED]->(p:Post)
            OPTIONAL MATCH (u)-[:LIKED]->(l:Post)
            OPTIONAL MATCH (u)-[:FAVORITE]->(c:City)
            RETURN 
                count(DISTINCT u) as total_users,
                count(DISTINCT p) as total_posts,
                count(DISTINCT l) as total_likes,
                count(DISTINCT c) as total_favorites
        """)
        return pd.DataFrame([dict(record) for record in result])

@st.cache_data(ttl=3600)
def get_city_activity_network():
    driver = get_neo4j_connection()
    with driver.session() as session:
        result = session.run("""
            MATCH (c:City)<-[:IN]-(p:Post)-[:INCLUDES]->(a:Activity)
            RETURN c.name as city, a.name as activity, count(*) as count
            ORDER BY count DESC
        """)
        return pd.DataFrame([dict(record) for record in result])

@st.cache_data(ttl=3600)
def get_user_network():
    driver = get_neo4j_connection()
    with driver.session() as session:
        result = session.run("""
            MATCH (u1:User)-[:LIKED]->(p:Post)<-[:CREATED]-(u2:User)
            WHERE u1 <> u2
            RETURN u1.user_id as user1, u2.user_id as user2, count(*) as interaction_count
            ORDER BY interaction_count DESC
        """)
        return pd.DataFrame([dict(record) for record in result])

@st.cache_data(ttl=3600)
def get_activity_network():
    driver = get_neo4j_connection()
    with driver.session() as session:
        result = session.run("""
            MATCH (a1:Activity)<-[:INCLUDES]-(p:Post)-[:INCLUDES]->(a2:Activity)
            WHERE a1 <> a2
            RETURN a1.name as activity1, a2.name as activity2, count(*) as co_occurrence
            ORDER BY co_occurrence DESC
            LIMIT 50
        """)
        return pd.DataFrame([dict(record) for record in result])

def create_network_graph(df, source_col, target_col, weight_col=None):
    G = nx.Graph()
    
    # Add edges with weights if provided
    if weight_col:
        edges = list(zip(df[source_col], df[target_col], df[weight_col]))
        G.add_weighted_edges_from(edges)
    else:
        edges = list(zip(df[source_col], df[target_col]))
        G.add_edges_from(edges)
    
    return G

def analyze_network(G):
    metrics = {
        'density': nx.density(G),
        'avg_clustering': nx.average_clustering(G),
        'avg_shortest_path': nx.average_shortest_path_length(G) if nx.is_connected(G) else 'Not connected',
        'degree_centrality': nx.degree_centrality(G),
        'betweenness_centrality': nx.betweenness_centrality(G),
        'closeness_centrality': nx.closeness_centrality(G)
    }
    return metrics

# Connect to DuckDB with GCS integration
@st.cache_resource
def get_connection():
    # Get GCS credentials from secrets
    gcs_creds = st.secrets["connections"]["gcs"]
    
    # Create credentials object
    credentials = service_account.Credentials.from_service_account_info(gcs_creds)
    
    # Initialize GCS client with credentials
    storage_client = storage.Client(credentials=credentials, project=gcs_creds["project_id"])
    
    # Get the bucket and blob
    bucket_name = st.secrets["gcs"]["bucket_name"]
    blob_name = st.secrets["gcs"]["events_file"]
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    # Download the file to a temporary location
    with tempfile.NamedTemporaryFile(delete=False, suffix='.duckdb') as temp_file:
        blob.download_to_filename(temp_file.name)
        temp_path = temp_file.name
    
    # Connect to the downloaded DuckDB file
    conn = duckdb.connect(temp_path)
    
    # Clean up the temporary file when the connection is closed
    def cleanup():
        if os.path.exists(temp_path):
            os.remove(temp_path)
    
    # Register cleanup function to be called when the connection is closed
    # conn.register_function("cleanup", cleanup)
    
    return conn

conn = get_connection()

# Load data directly from the dimensional model
@st.cache_data
def load_dimensional_data():
    # Load fact and dimension tables
    fact_events = conn.execute("""
        SELECT * FROM fact_events
    """).fetchdf()
    
    dim_location = conn.execute("SELECT * FROM dim_location").fetchdf()
    dim_category = conn.execute("SELECT * FROM dim_category").fetchdf()
    dim_date = conn.execute("SELECT * FROM dim_date").fetchdf()
    dim_audience = conn.execute("SELECT * FROM dim_audience").fetchdf()
    dim_organization = conn.execute("SELECT * FROM dim_organization").fetchdf()
    
    # Load full event details for search and display
    event_details = conn.execute("""
        SELECT 
            f.event_id,
            f.event_name,
            f.description,
            f.activity,
            f.is_free,
            f.price_details,
            f.time,
            f.excluded_days,
            f.website,
            sd.full_date AS start_date,
            ed.full_date AS end_date,
            sd.day_name AS day_of_week,
            sd.month,
            sd.year,
            sd.season,
            l.venue,
            l.address,
            l.address_neighbourhood,
            l.address_district,
            l.zip_code,
            l.city,
            l.country,
            l.latitude,
            l.longitude,
            c.category_name,
            a.audience_type,
            o.organization_name,
            o.contact_email,
            o.contact_phone
        FROM fact_events f
        LEFT JOIN dim_date sd ON f.start_date_id = sd.date_id
        LEFT JOIN dim_date ed ON f.end_date_id = ed.date_id
        LEFT JOIN dim_location l ON f.location_id = l.location_id
        LEFT JOIN dim_category c ON f.category_id = c.category_id
        LEFT JOIN dim_audience a ON f.audience_id = a.audience_id
        LEFT JOIN dim_organization o ON f.organization_id = o.organization_id
    """).fetchdf()
    
    return {
        "fact_events": fact_events,
        "dim_location": dim_location,
        "dim_category": dim_category,
        "dim_date": dim_date,
        "dim_audience": dim_audience,
        "dim_organization": dim_organization,
        "event_details": event_details
    }

# Load KPI data for pre-calculated metrics
@st.cache_data
def load_kpi_data():
    # Load KPI tables
    event_summary = conn.execute("SELECT * FROM event_summary").fetchdf()
    monthly_patterns = conn.execute("SELECT * FROM monthly_event_patterns").fetchdf()
    seasonal_events = conn.execute("SELECT * FROM seasonal_events").fetchdf()
    free_paid = conn.execute("SELECT * FROM free_event_distribution").fetchdf()
    events_by_district = conn.execute("SELECT * FROM events_by_district").fetchdf()
    top_neighborhoods = conn.execute("SELECT * FROM top_neighborhoods_by_city").fetchdf()
    category_analysis = conn.execute("SELECT * FROM free_paid_by_category").fetchdf()
    seasonal_popularity = conn.execute("SELECT * FROM seasonal_city_popularity").fetchdf()
    weekend_events = conn.execute("SELECT * FROM weekend_events").fetchdf()
    weekday_events = conn.execute("SELECT * FROM events_by_weekday").fetchdf()
    event_density = conn.execute("SELECT * FROM event_density_map").fetchdf()
    
    return {
        "summary": event_summary,
        "monthly": monthly_patterns,
        "seasonal": seasonal_events,
        "free_paid": free_paid,
        "districts": events_by_district,
        "neighborhoods": top_neighborhoods,
        "categories": category_analysis,
        "seasonal_pop": seasonal_popularity,
        "weekend": weekend_events,
        "weekday": weekday_events,
        "density": event_density
    }

# Load data
dim_data = load_dimensional_data()
kpi_data = load_kpi_data()

# Create unified event details dataframe with all necessary columns
events_df = dim_data["event_details"]

# Create sidebar filters
st.sidebar.markdown("<div class='filter-section'>", unsafe_allow_html=True)
st.sidebar.header("🔍 Filters")

# City filter
cities = ["All Cities", "Barcelona", "Madrid", "Paris"]
selected_city = st.sidebar.selectbox("Select City", cities)

# Season filter
seasons = ["All Seasons"] + events_df["season"].dropna().unique().tolist()
selected_season = st.sidebar.selectbox("Select Season", seasons)

# Date range filter
today = datetime.now().date()
date_range = st.sidebar.date_input(
    "Select Date Range",
    value=(today, today + timedelta(days=30)),
    min_value=today - timedelta(days=365),
    max_value=today + timedelta(days=365)
)

# Free/Paid filter
price_options = ["All Events", "Free Events", "Paid Events"]
selected_price = st.sidebar.selectbox("Event Price", price_options)

# Categories filter
all_categories = ["All Categories"] + sorted(events_df["category_name"].dropna().unique().tolist())
selected_category = st.sidebar.selectbox("Select Category", all_categories)


st.sidebar.markdown("</div>", unsafe_allow_html=True)

# Function to filter the events dataframe based on all selections
def filter_events(df):
    filtered_df = df.copy()
    
    # Apply filters
    if selected_city != "All Cities":
        filtered_df = filtered_df[filtered_df["city"] == selected_city]
        
    if selected_season != "All Seasons":
        filtered_df = filtered_df[filtered_df["season"] == selected_season]
        
    if selected_price != "All Events":
        if selected_price == "Free Events":
            filtered_df = filtered_df[filtered_df["is_free"].str.lower() == "free"]
        else:  # Paid Events
            filtered_df = filtered_df[filtered_df["is_free"].str.lower() == "paid"]
            
    if selected_category != "All Categories":
        filtered_df = filtered_df[filtered_df["category_name"] == selected_category]
        
   
    # Date range filter (if both dates selected)
    if len(date_range) == 2:
        start_date, end_date = date_range
        filtered_df = filtered_df[
            (pd.to_datetime(filtered_df["start_date"]) >= pd.Timestamp(start_date)) & 
            (pd.to_datetime(filtered_df["start_date"]) <= pd.Timestamp(end_date))
        ]
    
    return filtered_df

# Apply filters to get filtered events
filtered_events = filter_events(events_df)

# Create tabs for different views
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["Dashboard", "Event Map", "Event Search", "Calendar View", "About", "Social Analytics"])

# Main Dashboard Tab
with tab1:
    st.header("📊 Dashboard Overview")
    
    # Top row metrics
    st.markdown("<div class='metric-container'>", unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)
    
    # Show total events
    with col1:
        total_events = len(filtered_events)
        st.metric(label="Total Events", value=f"{total_events:,}")
    
    # Show free vs paid percentage
    with col2:
        free_events = len(filtered_events[filtered_events["is_free"].str.lower() == "free"])
        free_percentage = (free_events / total_events * 100) if total_events > 0 else 0
        st.metric(label="Free Events", value=f"{free_events:,}", delta=f"{free_percentage:.1f}%")
    
    # Show cities count
    with col3:
        cities_present = filtered_events["city"].nunique()
        st.metric(label="Cities", value=cities_present)
    
    # Show categories count
    with col4:
        categories_present = filtered_events["category_name"].nunique()
        st.metric(label="Categories", value=categories_present)
    
    st.markdown("</div>", unsafe_allow_html=True)
    
    # Create two columns for charts
    col1, col2 = st.columns(2)
    
    # Events by city - pie chart
    with col1:
        st.subheader("Events by City")
        city_count = filtered_events.groupby("city").size().reset_index(name="count")
        
        if not city_count.empty:
            fig = px.pie(
                city_count, 
                values="count", 
                names="city",
                color="city",
                color_discrete_map={
                    "Barcelona": "#E74C3C", 
                    "Madrid": "#F1C40F", 
                    "Paris": "#3498DB"
                },
                hole=0.4
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data available with current filters")
    
    # Free vs Paid Events by City
    with col2:
        st.subheader("Free vs Paid Events by City")
        
        # Create free vs paid data from filtered events
        free_paid_data = filtered_events.groupby(["city", "is_free"]).size().reset_index(name="event_count")
        if not free_paid_data.empty:
            # Format is_free column
            free_paid_data["is_free"] = free_paid_data["is_free"].str.capitalize()
            
            fig = px.bar(
                free_paid_data,
                x="city",
                y="event_count",
                color="is_free",
                barmode="group",
                color_discrete_map={
                    "Free": "#2ECC71",
                    "Paid": "#9B59B6"
                },
                text="event_count"
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data available with current filters")
    
    # Events by Season
    st.subheader("Events by Season")
    
    # Summarize filtered events by season
    seasonal_data = filtered_events.groupby(["season", "city"]).size().reset_index(name="event_count")
    
    if not seasonal_data.empty:
        # Create a more visually appealing seasonal chart
        fig = px.bar(
            seasonal_data,
            x="season",
            y="event_count",
            color="city",
            barmode="group",
            color_discrete_map={
                "Barcelona": "#E74C3C", 
                "Madrid": "#F1C40F", 
                "Paris": "#3498DB"
            },
            category_orders={"season": ["Winter", "Spring", "Summer", "Fall"]},
            labels={"event_count": "Number of Events", "season": "Season"}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No seasonal data available with current filters")
    
    # Third Row - Two column layout
    col1, col2 = st.columns(2)
    
    # Events by Day of Week
    with col1:
        st.subheader("Events by Day of Week")
        
        weekday_data = filtered_events.groupby(["day_of_week", "city"]).size().reset_index(name="event_count")
        
        if not weekday_data.empty:
            # Order days of week properly
            day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            
            # Create a mapping dictionary for sorting
            day_mapping = {day: i for i, day in enumerate(day_order)}
            
            # Add sorting column and sort
            weekday_data['day_order'] = weekday_data['day_of_week'].map(day_mapping)
            weekday_data = weekday_data.sort_values('day_order')
            
            fig = px.bar(
                weekday_data,
                x="day_of_week",
                y="event_count",
                color="city",
                color_discrete_map={
                    "Barcelona": "#E74C3C", 
                    "Madrid": "#F1C40F", 
                    "Paris": "#3498DB"
                },
                category_orders={"day_of_week": day_order},
                labels={"event_count": "Number of Events", "day_of_week": "Weekday"}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No weekday data available with current filters")
    
    # Top Categories
    with col2:
        st.subheader("Top Event Categories")
        
        # Create category distribution from filtered events
        cat_data = filtered_events.groupby(["category_name", "is_free"]).size().reset_index(name="event_count")
        
        if not cat_data.empty:
            # Get top 10 categories
            top_cats = cat_data.groupby("category_name")["event_count"].sum().reset_index()
            top_cats = top_cats.sort_values("event_count", ascending=False).head(10)
            top_cat_names = top_cats["category_name"].tolist()
            
            # Filter to only top categories
            cat_display = cat_data[cat_data["category_name"].isin(top_cat_names)]
            
            fig = px.bar(
                cat_display,
                x="category_name",
                y="event_count",
                color="is_free",
                barmode="stack",
                color_discrete_map={
                    "Free": "#2ECC71",
                    "Paid": "#9B59B6"
                },
                labels={"event_count": "Number of Events", "category_name": "Category"}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No category data available with current filters")
    
    # Fourth Row - Neighborhoods
    st.subheader("Top Neighborhoods for Events")
    
    # Create neighborhood distribution from filtered events
    neighborhood_data = filtered_events[filtered_events["address_neighbourhood"].notna()]
    neighborhood_counts = neighborhood_data.groupby(["city", "address_neighbourhood"]).size().reset_index(name="event_count")
    
    if not neighborhood_counts.empty:
        # Get top 5 neighborhoods for each city
        top_neighborhoods = (
            neighborhood_counts
            .sort_values(["city", "event_count"], ascending=[True, False])
            .groupby("city")
            .head(5)
        )
        
        # Add rank column
        top_neighborhoods["rank"] = top_neighborhoods.groupby("city")["event_count"].rank(ascending=False, method='first')
        
        fig = px.bar(
            top_neighborhoods,
            x="address_neighbourhood",
            y="event_count",
            color="city",
            color_discrete_map={
                "Barcelona": "#E74C3C", 
                "Madrid": "#F1C40F", 
                "Paris": "#3498DB"
            },
            facet_col="city" if selected_city == "All Cities" else None,
            text="event_count",
            labels={"event_count": "Number of Events", "address_neighbourhood": "Neighborhood"}
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No neighborhood data available with current filters")

# Event Map Tab
with tab2:
    st.header("🗺️ Event Location Map")
    
    # Filter out events without coordinates
    map_data = filtered_events[filtered_events["latitude"].notna() & filtered_events["longitude"].notna()]
    
    # Aggregate events at the same location
    if not map_data.empty:
        location_counts = map_data.groupby(["city", "latitude", "longitude"]).size().reset_index(name="event_count")
        
        # City center coordinates for initial view
        city_centers = {
            "All Cities": [41.3851, 2.1734, 6],  # Default to Barcelona
            "Barcelona": [41.3851, 2.1734, 12],
            "Madrid": [40.4168, -3.7038, 12],
            "Paris": [48.8566, 2.3522, 12]
        }
        
        center = city_centers[selected_city]
        
        # Create base map
        m = folium.Map(location=[center[0], center[1]], zoom_start=center[2], tiles="OpenStreetMap")
        
        # Add event markers
        for _, row in location_counts.iterrows():
            # Scale the radius based on event count (min 5, max 15)
            radius = min(15, max(5, row['event_count']/5))
            
            # Color based on city
            color = {
                "Barcelona": "#E74C3C",
                "Madrid": "#F1C40F", 
                "Paris": "#3498DB"
            }.get(row['city'], "#3498DB")
            
            # Add circle marker
            folium.CircleMarker(
                location=[row['latitude'], row['longitude']],
                radius=radius,
                color=color,
                fill=True,
                fill_color=color,
                fill_opacity=0.6,
                popup=f"{row['city']}: {row['event_count']} events"
            ).add_to(m)
        
        # Display map
        folium_static(m, width=1200, height=600)
        
        # Display events density explanation
        st.info("The map shows the concentration of events across the cities. Larger circles indicate more events at that location.")
    else:
        st.warning("No event locations available with the current filters. Try adjusting your selection.")

# Event Search Tab
with tab3:
    st.header("🔎 Find Your Perfect Event")
    
    # Add search box for event name on top of already filtered events
    search_query = st.text_input("Search events by name")
    
    search_results = filtered_events
    if search_query:
        search_results = search_results[
            (search_results["event_name"].str.contains(search_query, case=False, na=False))]
           
    # Sort options
    sort_options = ["Start Date (Ascending)", "Start Date (Descending)", "Event Name (A-Z)", "City (A-Z)"]
    selected_sort = st.selectbox("Sort by", sort_options)
    
    # Apply sorting
    if selected_sort == "Start Date (Ascending)":
        search_results = search_results.sort_values("start_date")
    elif selected_sort == "Start Date (Descending)":
        search_results = search_results.sort_values("start_date", ascending=False)
    elif selected_sort == "Event Name (A-Z)":
        search_results = search_results.sort_values("event_name")
    elif selected_sort == "City (A-Z)":
        search_results = search_results.sort_values("city")
    
    # Display filtered events
    if len(search_results) > 0:
        st.write(f"Found {len(search_results)} events matching your criteria")
        
        # Display events in expandable sections with pagination
        items_per_page = 10
        total_pages = (len(search_results) + items_per_page - 1) // items_per_page
        
        if total_pages > 1:
            page = st.number_input("Page", min_value=1, max_value=total_pages, value=1)
        else:
            page = 1
        
        start_idx = (page - 1) * items_per_page
        end_idx = min(start_idx + items_per_page, len(search_results))
        
        # Get the events for the current page
        page_events = search_results.iloc[start_idx:end_idx]
        
        for i, (_, event) in enumerate(page_events.iterrows()):
            with st.expander(f"{event['event_name']} - {event['city']}"):
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    st.markdown(f"**Category:** {event['category_name']}")
                    
                    # Format dates
                    start_date = pd.to_datetime(event['start_date']).strftime('%d %b %Y') if pd.notnull(event['start_date']) else 'N/A'
                    end_date = pd.to_datetime(event['end_date']).strftime('%d %b %Y') if pd.notnull(event['end_date']) else start_date
                    
                    if start_date == end_date:
                        st.markdown(f"**Date:** {start_date}")
                    else:
                        st.markdown(f"**Date Range:** {start_date} to {end_date}")
                    
                    st.markdown(f"**Time:** {event['time'] if pd.notnull(event['time']) else 'N/A'}")
                    
                    # Format price info
                    price_info = 'Free' if str(event['is_free']).lower() == 'free' else event['price_details'] if pd.notnull(event['price_details']) else 'Paid'
                    st.markdown(f"**Price:** {price_info}")
                    
                    # Add audience type if available
                    if pd.notnull(event['audience_type']):
                        st.markdown(f"**Audience:** {event['audience_type']}")
                    
                    
                with col2:
                    st.markdown(f"**Venue:** {event['venue'] if pd.notnull(event['venue']) else 'N/A'}")
                    
                    # Only show address if available
                    if pd.notnull(event['address']):
                        st.markdown(f"**Address:** {event['address']}")
                    
                    # Show district and neighborhood if available
                    location_info = []
                    if pd.notnull(event['address_district']):
                        location_info.append(f"District: {event['address_district']}")
                    if pd.notnull(event['address_neighbourhood']):
                        location_info.append(f"Neighborhood: {event['address_neighbourhood']}")
                    
                    if location_info:
                        st.markdown(f"**Location Details:** {', '.join(location_info)}")
                    
                    # Show small map for each event if coordinates available
                    if pd.notnull(event['latitude']) and pd.notnull(event['longitude']):
                        venue_map = folium.Map(location=[event['latitude'], event['longitude']], zoom_start=15, height=150)
                        folium.Marker(
                            location=[event['latitude'], event['longitude']],
                            popup=event['venue'] if pd.notnull(event['venue']) else event['event_name']
                        ).add_to(venue_map)
                        folium_static(venue_map, width=300, height=200)
                    
                    # Website link if available
                    if pd.notnull(event['website']):
                        st.markdown(f"[Visit Website]({event['website']})")
                    
                    
        
        # Show pagination controls if needed
        if total_pages > 1:
            cols = st.columns([1, 3, 1])
            with cols[1]:
                st.write(f"Page {page} of {total_pages}")
    else:
        st.warning("No events found matching your criteria. Try adjusting the filters.")

# Calendar View Tab
with tab4:
    st.header("📅 Event Calendar")
    
    # Select month for calendar
    current_month = datetime.now().month
    current_year = datetime.now().year
    months = list(calendar.month_name)[1:]
    selected_month = st.selectbox("Select Month", months, index=current_month-1)
    selected_month_num = months.index(selected_month) + 1
    
    # Filter events for selected month
    calendar_events = filtered_events.copy()
    calendar_events['start_date'] = pd.to_datetime(calendar_events['start_date'])
    month_events = calendar_events[
        (calendar_events['start_date'].dt.month == selected_month_num) &
        (calendar_events['start_date'].dt.year == current_year)
    ]
    
    # Create calendar layout
    st.subheader(f"Events Calendar - {selected_month} {current_year}")
    
    # Get the calendar for the selected month
    cal = calendar.monthcalendar(current_year, selected_month_num)
    
    # Display calendar as a grid
    cols = st.columns(7)
    for i, day_name in enumerate(["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]):
        cols[i].markdown(f"**{day_name}**")
    
    # Create a dictionary for date -> events count
    date_events = {}
    for _, event in month_events.iterrows():
        event_date = event['start_date'].date()
        if event_date in date_events:
            date_events[event_date] += 1
        else:
            date_events[event_date] = 1
    
    # Display calendar with events count
    for week in cal:
        cols = st.columns(7)
        for i, day in enumerate(week):
            if day == 0:
                cols[i].write("")
            else:
                date_key = datetime(current_year, selected_month_num, day).date()
                event_count = date_events.get(date_key, 0)
                
                # Style based on event count
                if event_count > 0:
                    cols[i].markdown(f"**{day}**")
                    cols[i].markdown(f"*{event_count} events*")
                else:
                    cols[i].write(day)
                    cols[i].write("")
    
    # Show list of events for the selected month
    if len(month_events) > 0:
        st.subheader(f"Events in {selected_month}")
        
        # Group by date
        month_events['date'] = month_events['start_date'].dt.date
        date_grouped = month_events.groupby('date')
        
        for date, events in date_grouped:
            st.write(f"**{date.strftime('%A, %B %d, %Y')}**")
            
            for _, event in events.iterrows():
                with st.expander(f"{event['event_name']} - {event['city']}"):
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        st.markdown(f"**Category:** {event['category_name']}")
                        st.markdown(f"**Time:** {event['time'] if pd.notnull(event['time']) else 'N/A'}")
                        
                        # Format price info
                        price_info = 'Free' if str(event['is_free']).lower() == 'free' else event['price_details'] if pd.notnull(event['price_details']) else 'Paid'
                        st.markdown(f"**Price:** {price_info}")
                        
                        
                    
                    with col2:
                        st.markdown(f"**Venue:** {event['organization_name'] if pd.notnull(event['organization_name']) else 'N/A'}")
                        st.markdown(f"**Address:** {event['address'] if pd.notnull(event['address']) else 'N/A'}")
                        
                        # Website link if available
                        if pd.notnull(event['website']):
                            st.markdown(f"[Visit Website]({event['website']})")
            
            st.write("---")
    else:
        st.info(f"No events found for {selected_month} {current_year} with the selected filters.")

# About Tab
with tab5:
    st.header("ℹ️ About Tripify Events Explorer")
    st.markdown("""
        Tripify Events Explorer allows users to explore and discover events happening in major European cities, specifically Barcelona, Madrid, and Paris. 
        Users can filter events based on various criteria such as city, season, date range, price (free/paid), and category.
        
        The app provides a dashboard overview with key metrics, visualizations of event distributions, and a map view to locate events geographically. 
        Users can also search for specific events and view them in a calendar format.
        
        ### Features:
    
        - **City Comparison**: See which city has the most active event scene during your visit
        - **Interactive Map**: Discover event hot spots in each city
        - **Seasonal Trends**: Plan your trip during the most eventful times
        - **Category Exploration**: Find events matching your interests
        - **Neighborhood Focus**: Target specific areas of each city
        - **Calendar View**: See what's happening each day of your visit
    """)
    
    
    st.markdown("### Technologies Used")
    st.markdown("""
        - **Streamlit**: For building the web application interface.
        - **DuckDB**: For efficient querying of the dimensional model.
        - **Plotly**: For creating interactive visualizations.
        - **Folium**: For rendering maps with event locations.
    """)
    
    st.markdown("### Contact")
    st.markdown("""
        If you have any questions or feedback about this application, please feel free to reach out!
    """)

# Social Analytics Tab (Neo4j Analysis)
with tab6:
    st.header("📊 Social Analytics")
    st.markdown("""
        This section provides insights from our social network data, showing how users interact with cities, activities, and posts.
    """)
    
    # User Activity Statistics
    st.subheader("User Activity Overview")
    user_stats = get_user_activity_stats()
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Users", f"{user_stats['total_users'].iloc[0]:,}")
    with col2:
        st.metric("Total Posts", f"{user_stats['total_posts'].iloc[0]:,}")
    with col3:
        st.metric("Total Likes", f"{user_stats['total_likes'].iloc[0]:,}")
    with col4:
        st.metric("Total Favorites", f"{user_stats['total_favorites'].iloc[0]:,}")
    
    # Network Analysis Section
    st.subheader("Network Analysis")
    st.markdown("""
        This section analyzes the relationships between users and activities to understand community engagement and activity patterns.
    """)
    
    # User Network Analysis
    st.markdown("### User Interaction Network")
    st.markdown("""
        Shows how users interact through likes and posts. A dense network indicates high user engagement, while clustering shows the formation of user communities.
    """)
    user_network = get_user_network()
    
    if not user_network.empty:
        G_user = create_network_graph(user_network, 'user1', 'user2', 'interaction_count')
        user_metrics = analyze_network(G_user)
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Network Density", f"{user_metrics['density']:.4f}")
            st.metric("Average Clustering", f"{user_metrics['avg_clustering']:.4f}")
        
        with col2:
            st.metric("Average Shortest Path", 
                     f"{user_metrics['avg_shortest_path']:.2f}" if isinstance(user_metrics['avg_shortest_path'], float) 
                     else user_metrics['avg_shortest_path'])
        
        # Plot user network with improved visualization
        plt.figure(figsize=(12, 8))
        pos = nx.spring_layout(G_user, k=1, iterations=50)
        
        # Draw nodes
        nx.draw_networkx_nodes(G_user, pos, 
                             node_size=100,
                             node_color='lightblue',
                             alpha=0.6)
        
        # Draw edges with varying widths based on weight
        edge_weights = [G_user[u][v]['weight'] for u, v in G_user.edges()]
        max_weight = max(edge_weights) if edge_weights else 1
        edge_widths = [2 * (w/max_weight) for w in edge_weights]
        
        nx.draw_networkx_edges(G_user, pos,
                             width=edge_widths,
                             alpha=0.4,
                             edge_color='gray')
        
        # Add some node labels (limit to top 10 nodes by degree)
        top_nodes = sorted(G_user.degree(), key=lambda x: x[1], reverse=True)[:10]
        labels = {node: f"User {i+1}" for i, (node, _) in enumerate(top_nodes)}
        nx.draw_networkx_labels(G_user, pos, labels, font_size=8)
        
        plt.title("User Interaction Network")
        plt.axis('off')
        st.pyplot(plt)
        plt.close()
    else:
        st.warning("No user interaction data available. This might be because there are no likes between different users yet.")
    
    # Activity Network Analysis
    st.markdown("### Activity Co-occurrence Network")
    st.markdown("""
        Reveals which activities are commonly planned together. Central activities are those that appear frequently in trip plans and connect different types of experiences.
    """)
    activity_network = get_activity_network()
    
    if not activity_network.empty:
        G_activity = create_network_graph(activity_network, 'activity1', 'activity2', 'co_occurrence')
        activity_metrics = analyze_network(G_activity)
        
        # Display top activities by centrality
        st.markdown("#### Most Central Activities")
        st.markdown("""
            Activities with high centrality scores are the most influential in trip planning, often serving as key attractions that connect other activities.
        """)
        centrality_df = pd.DataFrame({
            'Activity': list(activity_metrics['degree_centrality'].keys()),
            'Degree Centrality': list(activity_metrics['degree_centrality'].values()),
            'Betweenness Centrality': list(activity_metrics['betweenness_centrality'].values()),
            'Closeness Centrality': list(activity_metrics['closeness_centrality'].values())
        })
        st.dataframe(centrality_df.sort_values('Degree Centrality', ascending=False).head(10))
        
        # Plot activity network with improved visualization
        plt.figure(figsize=(15, 12))
        pos = nx.spring_layout(G_activity, k=2, iterations=50)
        
        # Draw nodes
        nx.draw_networkx_nodes(G_activity, pos,
                             node_size=200,
                             node_color='lightgreen',
                             alpha=0.7)
        
        # Draw edges with varying widths based on weight
        edge_weights = [G_activity[u][v]['weight'] for u, v in G_activity.edges()]
        max_weight = max(edge_weights) if edge_weights else 1
        edge_widths = [3 * (w/max_weight) for w in edge_weights]
        
        nx.draw_networkx_edges(G_activity, pos,
                             width=edge_widths,
                             alpha=0.4,
                             edge_color='gray')
        
        # Draw labels
        nx.draw_networkx_labels(G_activity, pos,
                              font_size=8,
                              font_weight='bold')
        
        plt.title("Activity Co-occurrence Network")
        plt.axis('off')
        st.pyplot(plt)
        plt.close()
    else:
        st.warning("No activity co-occurrence data available. This might be because there are no posts with multiple activities yet.")
    
    # Popular Cities Analysis
    st.subheader("Most Popular Cities")
    st.markdown("""
        Shows which cities generate the most user engagement and content creation, helping identify key markets and user preferences.
    """)
    popular_cities = get_popular_cities()
    
    fig = px.bar(
        popular_cities,
        x='city',
        y='post_count',
        title='Number of Posts by City',
        labels={'post_count': 'Number of Posts', 'city': 'City'},
        color='post_count',
        color_continuous_scale='Viridis'
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)
    
    # Popular Activities Analysis
    st.subheader("Top Activities")
    st.markdown("""
        Highlights the most frequently mentioned activities, indicating what experiences users find most valuable to share and recommend.
    """)
    popular_activities = get_popular_activities()
    
    fig = px.bar(
        popular_activities,
        x='activity',
        y='post_count',
        title='Most Popular Activities',
        labels={'post_count': 'Number of Posts', 'activity': 'Activity'},
        color='post_count',
        color_continuous_scale='Viridis'
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)
    
    # Additional Insights
    st.subheader("Key Insights")
    
    # Calculate some additional metrics
    total_activities = len(popular_activities)
    avg_posts_per_city = popular_cities['post_count'].mean()
    most_active_city = popular_cities.iloc[0]['city']
    most_popular_activity = popular_activities.iloc[0]['activity']
    
    st.markdown(f"""
        - There are **{total_activities}** different activities shared across the platform
        - On average, each city has **{avg_posts_per_city:.1f}** posts
        - **{most_active_city}** is the most active city in terms of posts
        - **{most_popular_activity}** is the most frequently mentioned activity
    """)

# Add footer
st.markdown("---")
st.markdown("© 2025 Tripify Events Explorer | Created for travelers, by travelers")
