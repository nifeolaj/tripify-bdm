# Databricks notebook source
# COMMAND ----------

# Get parameters from Airflow
dbutils.widgets.text("users_path", "")
dbutils.widgets.text("posts_path", "")
dbutils.widgets.text("likes_path", "")
dbutils.widgets.text("output_path", "")

users_path = dbutils.widgets.get("users_path")
posts_path = dbutils.widgets.get("posts_path")
likes_path = dbutils.widgets.get("likes_path")
output_path = dbutils.widgets.get("output_path")

print(f"Users path: {users_path}")
print(f"Posts path: {posts_path}")
print(f"Likes path: {likes_path}")
print(f"Output path: {output_path}")

# COMMAND ----------

# Install required packages
%pip install neo4j pandas

# COMMAND ----------

# Import required libraries
from pyspark.sql.functions import (
    col, explode, array, lit, struct, from_json, split, current_timestamp, to_json, monotonically_increasing_id
)
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType
import logging
import neo4j
import pandas as pd
from neo4j import GraphDatabase
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

def read_parquet_file(path):
    """Read a Parquet file and return the DataFrame"""
    print(f"Reading from: {path}")
    try:
        df = spark.read.format("parquet").load(path)
        print(f"Successfully read file with {df.count()} rows")
        return df
    except Exception as e:
        print(f"Error reading file: {str(e)}")
        raise

# COMMAND ----------

# Process users data
print("\n=== Processing Users ===")
users_df = read_parquet_file(users_path)

# Process users data for nodes
print("Generating user nodes...")
user_nodes = users_df.select(
    col("user_id").alias("id"),
    lit("User").alias("labels"),
    struct(
        col("user_id"),
        col("name"),
        col("email"),
        col("username"),
        col("bio"),
        col("join_date"),
        col("location"),
        col("profile_picture")
    ).alias("properties")
)

# Generate FAVORITE relationships
print("Generating favorite relationships...")
favorite_edges = users_df.select(
    col("user_id").alias("source"),
    explode(split(col("favorite_cities"), ",")).alias("target"),
    lit("FAVORITE").alias("type"),
    struct(
        lit(current_timestamp()).alias("created_at")
    ).alias("properties")
)

# Write user files
print("Writing user files...")
user_nodes.write.format("parquet").mode('overwrite').save(f"{output_path}/user_nodes")
favorite_edges.write.format("parquet").mode('overwrite').save(f"{output_path}/favorite_edges")
print("Users processing completed successfully")

# COMMAND ----------

# Process posts data
print("\n=== Processing Posts ===")
posts_df = read_parquet_file(posts_path)

# Define schema for daily_plans
activity_schema = StructType([
    StructField("time", StringType(), True),
    StructField("activity", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("location", StringType(), True),
    StructField("description", StringType(), True)
])

day_schema = StructType([
    StructField("day", IntegerType(), True),
    StructField("activities", ArrayType(activity_schema), True)
])

daily_plans_schema = ArrayType(day_schema)

print("Parsing daily plans...")
# Process posts data for nodes
posts_df = posts_df.withColumn(
    "daily_plans_parsed",
    from_json(col("daily_plans"), daily_plans_schema)
)

print("Generating post nodes...")
# Generate Post nodes
post_nodes = posts_df.select(
    col("post_id").alias("id"),
    lit("Post").alias("labels"),
    struct(
        col("post_id"),
        col("title"),
        col("description"),
        col("start_date"),
        col("end_date"),
        col("total_cost"),
        col("created_at"),
        col("likes_count"),
        col("comments_count")
    ).alias("properties")
)

print("Writing post nodes...")
post_nodes.write.format("parquet").mode('overwrite').save(f"{output_path}/post_nodes")
print("Post nodes written successfully")

print("Generating activity nodes...")
# Generate Activity nodes
activity_nodes = posts_df.select(
    explode("daily_plans_parsed").alias("day")
).select(
    explode("day.activities").alias("activity")
).select(
    col("activity.activity").alias("id"),
    lit("Activity").alias("labels"),
    struct(
        col("activity.activity").alias("name"),
        col("activity.description")
    ).alias("properties")
).distinct()

print("Writing activity nodes...")
activity_nodes.write.format("parquet").mode('overwrite').save(f"{output_path}/activity_nodes")
print("Activity nodes written successfully")

print("Generating relationships...")
# Generate CREATED relationships
created_edges = posts_df.select(
    col("user_id").alias("source"),
    col("post_id").alias("target"),
    lit("CREATED").alias("type"),
    struct(
        lit(current_timestamp()).alias("created_at")
    ).alias("properties")
)

# Generate IN_CITY relationships
in_city_edges = posts_df.select(
    col("post_id").alias("source"),
    col("city").alias("target"),
    lit("IN_CITY").alias("type"),
    struct(
        lit(current_timestamp()).alias("created_at")
    ).alias("properties")
)

# Generate INCLUDES relationships
includes_edges = posts_df.select(
    col("post_id").alias("source"),
    explode("daily_plans_parsed").alias("day_data")
).select(
    col("source"),
    col("day_data.day").alias("day_number"),
    explode("day_data.activities").alias("activity")
).select(
    col("source"),
    col("activity.activity").alias("target"),
    lit("INCLUDES").alias("type"),
    struct(
        col("day_number").alias("day"),
        col("activity.time").alias("time"),
        col("activity.duration").alias("duration"),
        col("activity.location").alias("location"),
        lit(current_timestamp()).alias("created_at")
    ).alias("properties")
)

print("Writing relationship files...")
created_edges.write.format("parquet").mode('overwrite').save(f"{output_path}/created_edges")
in_city_edges.write.format("parquet").mode('overwrite').save(f"{output_path}/in_city_edges")
includes_edges.write.format("parquet").mode('overwrite').save(f"{output_path}/includes_edges")
print("All relationship files written successfully")
print("Posts processing completed successfully")

# COMMAND ----------

# Process likes data
print("\n=== Processing Likes ===")
likes_df = read_parquet_file(likes_path)

print("Generating like relationships...")
# Process likes data for edges
liked_edges = likes_df.select(
    col("user_id").alias("source"),
    col("post_id").alias("target"),
    lit("LIKED").alias("type"),
    struct(
        col("like_id"),
        col("created_at")
    ).alias("properties")
)

print("Writing like relationships...")
# Write edge file
liked_edges.write.format("parquet").mode('overwrite').save(f"{output_path}/liked_edges")
print("Likes processing completed successfully")

# COMMAND ----------

# Create city nodes
print("\n=== Processing Cities ===")
cities = ["Paris", "Rome", "Barcelona", "Madrid"]

# Create city nodes DataFrame
city_nodes = spark.createDataFrame([
    (city, "City", {"name": city})
    for city in cities
], ["id", "labels", "properties"])

print("Writing city nodes...")
city_nodes.write.format("parquet").mode('overwrite').save(f"{output_path}/city_nodes")
print("City nodes written successfully")

# COMMAND ----------

def convert_and_upload_to_neo4j():
    """Convert Parquet files to Neo4j format and upload to Neo4j"""
    uri = "neo4j://34.0.198.31:7687"
    user = "neo4j"
    password = "pass"
    
    logging.info("Starting Neo4j upload process...")
    driver = GraphDatabase.driver(uri, auth=(user, password))
    
    try:
        with driver.session() as session:
            # Process all node files first
            logging.info("Processing node files...")
            for node_file in dbutils.fs.ls(output_path):
                if "_nodes" in node_file.path:
                    logging.info(f"Processing node file: {node_file.path}")
                    df = spark.read.parquet(node_file.path)
                    node_type = node_file.name.split("_")[0].upper()
                    logging.info(f"Found {df.count()} {node_type} nodes to process")
                    
                    # Create nodes in batches
                    for batch in df.collect():
                        properties = batch['properties']
                        if hasattr(properties, 'asDict'):
                            properties = properties.asDict()
                        
                        query = f"""
                        MERGE (n:{node_type} {{id: $id}})
                        SET n += $properties
                        """
                        session.run(query, id=batch['id'], properties=properties)
                    logging.info(f"Completed uploading {node_type} nodes")
            
            # Process all edge files after nodes are created
            logging.info("Processing edge files...")
            for edge_file in dbutils.fs.ls(output_path):
                if "_edges" in edge_file.path:
                    logging.info(f"Processing edge file: {edge_file.path}")
                    df = spark.read.parquet(edge_file.path)
                    rel_type = edge_file.name.split("_")[0].upper()
                    logging.info(f"Found {df.count()} {rel_type} relationships to process")
                    
                    # Process relationships in batches of 1000
                    batch_size = 1000
                    total_relationships = df.count()
                    processed = 0
                    
                    while processed < total_relationships:
                        batch_df = df.limit(batch_size).offset(processed)
                        batch_data = batch_df.collect()
                        
                        if not batch_data:
                            break
                            
                        # Create a batch query for relationships
                        query = f"""
                        UNWIND $batch as row
                        MATCH (source {{id: row.source}})
                        MATCH (target {{id: row.target}})
                        MERGE (source)-[r:{rel_type}]->(target)
                        SET r += row.properties
                        """
                        
                        # Prepare batch data
                        batch_params = []
                        for row in batch_data:
                            properties = row['properties']
                            if hasattr(properties, 'asDict'):
                                properties = properties.asDict()
                            batch_params.append({
                                'source': row['source'],
                                'target': row['target'],
                                'properties': properties
                            })
                        
                        try:
                            session.run(query, batch=batch_params)
                            processed += len(batch_data)
                            logging.info(f"Processed {processed}/{total_relationships} {rel_type} relationships")
                        except Exception as e:
                            logging.error(f"Error processing batch at offset {processed}: {str(e)}")
                            raise
                    
                    logging.info(f"Completed uploading {rel_type} relationships")
    
    except Exception as e:
        logging.error(f"Error in Neo4j upload process: {str(e)}")
        raise
    finally:
        logging.info("Closing Neo4j connection...")
        driver.close()
        logging.info("Neo4j upload process completed")

# COMMAND ----------

# Execute conversion and upload
try:
    convert_and_upload_to_neo4j()
    print("Successfully processed and uploaded data to Neo4j")
except Exception as e:
    print(f"Error in processing: {str(e)}")
    raise 