# Databricks notebook source
# COMMAND ----------

# Import required libraries
import pandas as pd
import numpy as np
from pyspark.sql.functions import (
    concat_ws, to_timestamp, date_format, lit, col, explode, create_map,
    lower, expr, regexp_replace, split, regexp_extract, when, size
)
from itertools import chain

# COMMAND ----------

# Get parameters from Airflow
dbutils.widgets.text("input_path", "")
dbutils.widgets.text("output_path", "")
dbutils.widgets.text("transport_type", "")

input_path = dbutils.widgets.get("input_path")
output_path = dbutils.widgets.get("output_path")
transport_type = dbutils.widgets.get("transport_type")

print(f"Input path: {input_path}")
print(f"Output path: {output_path}")
print(f"Transport type: {transport_type}")

# COMMAND ----------

def get_city_country_map():
    """Return mapping of cities to countries"""
    city_to_country = {
        "barcelona": "Spain",
        "madrid": "Spain",
        "rome": "Italy",
        "paris": "France"
    }
    return create_map([lit(x) for x in chain(*city_to_country.items())])

def extract_city_name(station_col):
    """Extract clean city name from station string"""
    return regexp_replace(
        regexp_extract(lower(station_col), "^([^/\\s]+)(?:/.*|\\s.*|$)", 1),
        "[^a-z]", ""
    )

def preprocess_bus(df):
    """Process bus fare data from parquet files"""
    city_country_map = get_city_country_map()
    
    df_exploded = df.withColumn("trip", explode("data"))
    df_normalized = df_exploded.withColumn("Departure_City", lower(col("departure_city"))) \
                            .withColumn("Arrival_City", lower(col("arrival_city")))

    df_with_reformatted_date = df_normalized.withColumn(
        "departure_date_formatted",
        date_format(to_timestamp(col("departure_date"), "ddMMyyyy"), "yyyy-MM-dd")
    )

    df_with_datetime = df_with_reformatted_date.withColumn(
        "departure_datetime_str",
        concat_ws(" ", col("departure_date_formatted"), col("trip.`Departure Time`"))
    ).withColumn(
        "departure_timestamp",
        to_timestamp("departure_datetime_str", "yyyy-MM-dd HH:mm")
    )

    df_with_duration = df_with_datetime.withColumn(
        "cleaned_duration",
        regexp_replace(col("trip.Duration"), " h$", "")  
    ).withColumn(
        "duration_hours",
        split(col("cleaned_duration"), ":").getItem(0).cast("int")
    ).withColumn(
        "duration_minutes",
        split(col("cleaned_duration"), ":").getItem(1).cast("int")
    ).withColumn(
        "total_duration_minutes",
        col("duration_hours") * 60 + col("duration_minutes")
    )

    df_with_arrival = df_with_duration.withColumn(
        "arrival_timestamp",
        expr(f"departure_timestamp + (total_duration_minutes * INTERVAL '1' MINUTE)")
    )

    df_final = df_with_arrival.select(
        lit("busfare").alias("Type"),
        lit("Alsabus").alias("Company"),
        extract_city_name(col("trip.Departure")).alias("Departure"),
        lower(city_country_map[col("Departure_City")]).alias("Departure_Country"),
        lower(col("trip.Departure")).alias("Departure_Station"),
        extract_city_name(col("trip.Arrival")).alias("Arrival"),
        lower(city_country_map[col("Arrival_City")]).alias("Arrival_Country"),
        lower(col("trip.Arrival")).alias("Arrival_Station"),
        when(col("departure_timestamp").isNotNull(),
             date_format("departure_timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS+02:00")
        ).otherwise(None).alias("Departure_Time"),
        when(col("arrival_timestamp").isNotNull(),
             date_format("arrival_timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS+02:00")
        ).otherwise(None).alias("Arrival_Time"),
        col("cleaned_duration").alias("Duration"),
        col("trip.Price").alias("Price"),
        lit("euro").alias("Currency")
    )

    return df_final

def preprocess_train(df):
    """Process train fare data from parquet files"""
    city_country_map = get_city_country_map()
    
    df_exploded = df.withColumn("trip", explode("data"))
    df_normalized = df_exploded.withColumn("Departure_City", lower(col("departure_city"))) \
                            .withColumn("Arrival_City", lower(col("arrival_city")))

    df_with_reformatted_date = df_normalized.withColumn(
        "departure_date_formatted",
        date_format(to_timestamp(col("departure_date"), "ddMMyyyy"), "yyyy-MM-dd")
    )

    # Clean the departure time by removing the 'h' suffix
    df_with_clean_time = df_with_reformatted_date.withColumn(
        "clean_departure_time",
        regexp_replace(col("trip.`Departure Time`"), "\\s*h$", "")
    )

    df_with_datetime = df_with_clean_time.withColumn(
        "departure_datetime_str",
        concat_ws(" ", col("departure_date_formatted"), col("clean_departure_time"))
    ).withColumn(
        "departure_timestamp",
        to_timestamp("departure_datetime_str", "yyyy-MM-dd HH:mm")
    )

    df_with_duration = df_with_datetime.withColumn(
        "cleaned_duration",
        regexp_replace(col("trip.Duration"), "h|mins", "")  
    ).withColumn(
        "duration_parts",
        split(col("cleaned_duration"), " ")
    ).withColumn(
        "total_duration_minutes",
        when(size(col("duration_parts")) > 1,
             col("duration_parts").getItem(0).cast("int") * 60 + 
             col("duration_parts").getItem(1).cast("int")
        ).otherwise(
             col("duration_parts").getItem(0).cast("int") * 60
        )
    )

    df_with_arrival = df_with_duration.withColumn(
        "arrival_timestamp",
        expr(f"departure_timestamp + (total_duration_minutes * INTERVAL '1' MINUTE)")
    )

    # Format timestamps with proper timezone and handle nulls
    df_final = df_with_arrival.select(
        lit("trainfare").alias("Type"),
        lit("EuroRail").alias("Company"),
        extract_city_name(col("trip.Departure")).alias("Departure"),
        lower(city_country_map[col("Departure_City")]).alias("Departure_Country"),
        lower(col("trip.Departure")).alias("Departure_Station"),
        extract_city_name(col("trip.Arrival")).alias("Arrival"),
        lower(city_country_map[col("Arrival_City")]).alias("Arrival_Country"),
        lower(col("trip.Arrival")).alias("Arrival_Station"),
        when(col("departure_timestamp").isNotNull(),
             date_format("departure_timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS+02:00")
        ).otherwise(None).alias("Departure_Time"),
        when(col("arrival_timestamp").isNotNull(),
             date_format("arrival_timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS+02:00")
        ).otherwise(None).alias("Arrival_Time"),
        col("trip.Duration").alias("Duration"),
        col("trip.Price").alias("Price"),
        lit("euro").alias("Currency")
    )

    return df_final

def preprocess_air(df):
    """Process air fare data from parquet files"""
    city_country_map = get_city_country_map()
    
    df_exploded = df.withColumn("trip", explode("data"))
    df_normalized = df_exploded.withColumn("Departure_City", lower(col("departure_city"))) \
                            .withColumn("Arrival_City", lower(col("arrival_city")))

    df_with_reformatted_date = df_normalized.withColumn(
        "departure_date_formatted",
        date_format(to_timestamp(col("departure_date"), "ddMMyyyy"), "yyyy-MM-dd")
    )

    df_with_datetime = df_with_reformatted_date.withColumn(
        "departure_datetime_str",
        concat_ws(" ", col("departure_date_formatted"), col("trip.`Departure Time`"))
    ).withColumn(
        "departure_timestamp",
        to_timestamp("departure_datetime_str", "yyyy-MM-dd HH:mm")
    )

    df_with_duration = df_with_datetime.withColumn(
        "duration_minutes",
        regexp_extract(col("trip.Duration"), "(\\d+)", 1).cast("int")
    )

    df_with_arrival = df_with_duration.withColumn(
        "arrival_timestamp",
        expr(f"departure_timestamp + (duration_minutes * INTERVAL '1' MINUTE)")
    )

    df_final = df_with_arrival.select(
        lit("airfare").alias("Type"),
        col("trip.Airline").alias("Company"),
        extract_city_name(col("trip.Departure")).alias("Departure"),
        lower(city_country_map[col("Departure_City")]).alias("Departure_Country"),
        lower(col("trip.Departure")).alias("Departure_Station"),
        extract_city_name(col("trip.Arrival")).alias("Arrival"),
        lower(city_country_map[col("Arrival_City")]).alias("Arrival_Country"),
        lower(col("trip.Arrival")).alias("Arrival_Station"),
        when(col("departure_timestamp").isNotNull(),
             date_format("departure_timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS+02:00")
        ).otherwise(None).alias("Departure_Time"),
        when(col("arrival_timestamp").isNotNull(),
             date_format("arrival_timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS+02:00")
        ).otherwise(None).alias("Arrival_Time"),
        col("trip.Duration").alias("Duration"),
        col("trip.Price").alias("Price"),
        lit("euro").alias("Currency")
    )

    return df_final 

# COMMAND ----------

# Read input Parquet file
print(f"Reading from: {input_path}")
try:
    df = spark.read.format("parquet").load(input_path)
    print(f"Successfully read input file with {df.count()} rows")
except Exception as e:
    print(f"Error reading input file: {str(e)}")
    raise

# Select preprocessing function based on transport type
preprocess_func = {
    'air': preprocess_air,
    'bus': preprocess_bus,
    'train': preprocess_train
}[transport_type]

# Process the data
processed_df = preprocess_func(df)
print(f"Processed data has {processed_df.count()} rows")

# Write the processed data
print(f"Writing to: {output_path}")
try:
    processed_df.write.format("parquet").mode('append').save(output_path)
    print("Successfully wrote processed data")
except Exception as e:
    print(f"Error writing output file: {str(e)}")
    raise 