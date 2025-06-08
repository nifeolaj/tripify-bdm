# Web App Replication Steps

## To run locally,

Step 1: Create a .env file at the base directory. It should contain the following credentials.

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

Step 2: Generate a service account key from Google Cloud.

Assuming that GCS has already been setup - generate a service account keyfile, and store it in the directory app.py is located.

Step 3: Set Up Your Environment

Go the directory app.py is stored, and create a Python virtual environment.

```
python -m venv venv
```

Then, activate the virtual environment

```
On Windows:
venv\Scripts\activate
On macOS/Linux:
source venv/bin/activate
```

Step 4: Install Required Packages

```
pip install -r requirements.txt
```

Step 5: Run the app

```
python app.py
```

Step 6: Open Web App

The web app should be served at localhost:5001 unless stated otherwise.

NOTE that this will also require access to serverless instances of MongoDB, Pinecone, Neo4J as well as a running Kafka to work properly, although there are fallback mechanisms in place just in case those don't work.
