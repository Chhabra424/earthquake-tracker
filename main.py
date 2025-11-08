import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.bigquery import LoadJobConfig

# Load credentials
creds = service_account.Credentials.from_service_account_file("creds.json")
client = bigquery.Client(credentials=creds, project=creds.project_id)

# Fetch earthquake data from the last 24 hours
url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
response = requests.get(url)
data = response.json()

# Process data into a DataFrame
rows = []
for feature in data['features']:
    props = feature['properties']
    geom = feature['geometry']

    rows.append({
        "id": feature["id"],
        "place": props.get("place"),
        "time": pd.to_datetime(props.get("time"), unit='ms'),
        "magnitude": props.get("mag"),
        "longitude": geom["coordinates"][0],
        "latitude": geom["coordinates"][1],
        "depth": geom["coordinates"][2]
    })

df = pd.DataFrame(rows)

# Show how many rows will be uploaded
print(f"Uploading {len(df)} rows to BigQuery...")

# Push to BigQuery (append mode)
table_id = f"{creds.project_id}.earthquake_data.global_quakes"
job_config = LoadJobConfig(write_disposition="WRITE_APPEND")  # append new rows
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()

print("✅ Earthquake data uploaded to BigQuery successfully.")
