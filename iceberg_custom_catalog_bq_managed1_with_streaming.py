"""
This code demonstrates streaming data into a BigQuery table using the insert_rows_json method of the BigQuery client library in Python.
This method is suitable for small to medium-sized streaming inserts. For very high-throughput scenarios, consider using the BigQuery Storage Write API.
"""
from google.cloud import bigquery
import datetime
import random
import time
import os

os.environ["GCLOUD_PROJECT"] = "trim-strength-477307-h0"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\\Users\\debu\\AppData\\Roaming\\gcloud\\legacy_credentials\\nitipradhan17@gmail.com\\adc.json"

# --- Configuration ---
PROJECT_ID = "trim-strength-477307-h0"  # Replace with your Google Cloud Project ID
DATASET_ID = "customer"      # Replace with your BigQuery Dataset ID
TABLE_ID = "customer_details_bq_native_table_streaming"          # Replace with your BigQuery Table ID

# Schema for the target BigQuery table
# TABLE_SCHEMA = [
#     bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
#     bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
#     bigquery.SchemaField("user_id", "INTEGER", mode="REQUIRED"),
#     bigquery.SchemaField("value", "FLOAT", mode="NULLABLE"),
# ]

TABLE_SCHEMA = [
    bigquery.SchemaField("id", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("age", "INTEGER", mode="REQUIRED")
]


def create_table_if_not_exists(client, project_id, dataset_id, table_id, schema):
    """Creates the BigQuery table if it does not already exist."""
    table_ref = client.dataset(dataset_id, project=project_id).table(table_id)
    try:
        client.get_table(table_ref)  # Check if table exists
        print(f"Table {table_id} already exists.")
    except Exception:
        print(f"Creating table {table_id}...")
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"Table {table_id} created successfully.")

def generate_mock_data():
    """Generates a single mock data row for streaming."""
    now = datetime.datetime.now(datetime.timezone.utc)
    event_types = ["login", "logout", "purchase", "view_item"]
    sample_names = ["Josh", "Rosh", "Kosh", "Trosh"]
    # return {
    #     "timestamp": now.isoformat(),
    #     "event_type": random.choice(event_types),
    #     "user_id": random.randint(1000, 9999),
    #     "value": round(random.uniform(1.0, 1000.0), 2) if random.random() > 0.5 else None,
    # }
    return {
        "id": random.randint(1000, 9999),
        "name": random.choice(sample_names),
        "age": random.randint(10, 100)
    }

def stream_data_to_bigquery(client, project_id, dataset_id, table_id, num_rows=5):
    """Streams a specified number of mock data rows to BigQuery."""
    table_full_id = f"{project_id}.{dataset_id}.{table_id}"
    print(f"Streaming {num_rows} rows to {table_full_id}...")

    rows_to_insert = []
    for _ in range(num_rows):
        rows_to_insert.append(generate_mock_data())

    errors = client.insert_rows_json(table_full_id, rows_to_insert)

    if errors:
        print(f"Encountered errors while inserting rows: {errors}")
    else:
        print(f"Successfully inserted {num_rows} rows.")

if __name__ == "__main__":
    # Initialize BigQuery client
    client = bigquery.Client(project=PROJECT_ID)

    # Ensure the table exists
    create_table_if_not_exists(client, PROJECT_ID, DATASET_ID, TABLE_ID, TABLE_SCHEMA)

    # Stream data in a loop
    for i in range(10):  # Stream data multiple times
        print(f"\n--- Streaming batch {i+1} ---")
        stream_data_to_bigquery(client, PROJECT_ID, DATASET_ID, TABLE_ID, num_rows=random.randint(1, 10))
        time.sleep(2) # Simulate some delay between batches

    print("\nBigQuery streaming demo completed.")