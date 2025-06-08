# Airflow Replication Steps

### Local Setup

To run the program, clone the project repository and set up Apache Airflow using Astro CLI on your local machine.
Refer to [this documentation](https://www.astronomer.io/docs/astro/cli/install-cli/)  for setup guidance. 
  
Start Airflow services:
```bash
  astro dev start
  ```

This will open [http://localhost:8080](http://localhost:8080) in a browser that will give access to the Airflow UI and enable the required DAGs.

Make sure to move the dags folder in to the dags folder of the Airflow PATH that was set up, and before running Airflow, install the required dependencies as well with:

```
pip install -r requirements.txt
```

In addition, create a .env file at the base directory. It should contain the following credentials.

```
GEMINI_API_KEY=
PINECONE_API_KEY=
PINECONE_ENVIRONMENT=
MONGODB_CONNECTION_STRING=
MONGO_URI=
NEO4J_URI=
NEO4J_USER=
NEO4J_PASSWORD=
GRAPH_API_URL=
GOOGLE_APPLICATION_CREDENTIALS=
GCS_BUCKET_NAME=
KAFKA_BOOTSTRAP_SERVERS=
SCRAPER_SERVER_URL=

```

Finally, generate a service account key from Google Cloud.

Assuming that GCS has already been setup - generate a service account keyfile, and store it in the Airflow PATH.


### Deployment Setup

In our case, we deployed Airflow on Google Compute Engine. For this set up a GCS account, and follow the steps to create a GCE instance here: https://cloud.google.com/compute/docs/instances/create-start-instance

Import the files into the GCE instance via SSH, and run the following:

```
docker compose build --no-cache

docker compose up
```

This should set up the Airflow instance that will be accessible via the External IP of the instance at port 8080. Similar to above, this will give access to the Airflow UI where we can run the DAGs.
