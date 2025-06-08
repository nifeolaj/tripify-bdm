# Streamlit Replication Steps

To explore the web application, visit: https://tripify-dashboard.streamlit.app/

## To run locally,

Step 1: Create a secrets file at .streamlit/secrets.toml. This should contain the following credentials.

```
[connections.gcs]
type = 
project_id = 
private_key_id = 
private_key = 
client_email = 
client_id = 
auth_uri = 
token_uri = 
auth_provider_x509_cert_url = 
client_x509_cert_url = 
universe_domain = 

[gcs]
bucket_name = 
events_file = 

[neo4j]
uri = 
user = 
password = 
```

Step 2: Set Up Your Environment

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

Step 3: Install Required Packages

```
pip install -r requirements.txt
```

Step 4: Run the app

```
streamlit run app.py
```
