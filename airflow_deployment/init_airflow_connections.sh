#!/bin/bash
# Wait for the Airflow DB to be ready
sleep 10

# Create Spark connection
airflow connections add 'spark_default' \
    --conn-type 'spark' \
    --conn-host 'spark://spark-master:7077'

# Create Databricks connection
airflow connections add 'databricks_default' \
    --conn-type 'databricks' \
    --conn-host '<ADD_HOST>' \
    --conn-password "<PASSWORD>
