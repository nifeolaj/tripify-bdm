# Tripify BDM 

## Instructions on how to use the application

To run the program, clone the project repository and set up Apache Airflow using Astro CLI on your local machine.
Refer to [this documentation](https://www.astronomer.io/docs/astro/cli/install-cli/)  for setup guidance
  
Start Airflow services:
```bash
  astro dev start
  ```

This will open [http://localhost:8080](http://localhost:8080) in a browser that will give access to the Airflow UI and enable the required DAGs.

For setting up Kafka use docker 

Run the Docker Container: 
```
docker run -d -p 9092:9092 --name broker apache/kafka:latest
```
Create three topics in kafka - `airfare` , `trainfare`, `busfare` , `airfare_cleaned`, `trainfare_cleaned` and `busfare_cleaned` using the given code just change the topic name
```
docker exec -it broker opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--create \
--topic airfare \  
--partitions 3 \
--replication-factor 1

```
Run the following files to start the `kafka producer`:

- `alsa-scraper.py`
- `renfe-scraper.py`
- `flightfares.py`

To connect with Google Cloud services from your local machine:
1. Download the service account credentials `.json` file from the Google Cloud Console.
2. Update the path variable to point to the location of the json credentials:
```bash
   export GOOGLE_APPLICATION_CREDENTIALS="~/bdm-scraper/your-credentials-file.json"
```
Generate API Keys for
- `Google Gemini API`
- `Pincone`
- `MongoDB`
- `Serpe API`
- `BlaBlaCar`
- `Flixbus`

Run the `kafka consumer`using the `kafka_consumer.py` script for separate topics
Run `BusFareStreamsApp.java` script for preprocessing the fares generated through producer.
Run `flask_website/app.py` to start the app locally.


Run the following Airflow DAGs to extract the data from external sources and load the raw data into the **temporal landing zone**:
- `events_api_dag`
- `events_dag`
- `llm_itinerary_dag`

After storing in the temporal landing zone, run the following DAGs to process and store it in the **persistent landing zone**:
- `process_events_dag.py`
- `process_itineraries_dag.py`
- `process_transport_dag.py`

The DAG runs and task statuses can be monitored using the Airflow UI. 


