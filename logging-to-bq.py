from google.cloud import logging
from google.cloud import bigquery
from datetime import datetime
import pytz

def query_and_sink_gcs_logs(project_id, bucket_name, timestamp, bigquery_dataset, bigquery_table):
    """
    Queries Cloud Logging for GCS bucket logs and sinks them to BigQuery.

    Args:
        project_id (str): Your Google Cloud project ID.
        bucket_name (str): The name of the GCS bucket.
        timestamp (datetime): The timestamp to filter logs after.
        bigquery_dataset (str): The BigQuery dataset to sink logs to.
        bigquery_table (str): The BigQuery table to sink logs to.
    """
    try:
        logging_client = logging.Client(project=project_id)
        bigquery_client = bigquery.Client(project=project_id)

        # Ensure timestamp is in UTC
        if timestamp.tzinfo is None or timestamp.tzinfo.utcoffset(timestamp) is None:
            timestamp = pytz.utc.localize(timestamp)

        log_filter = (
            f'resource.type="gcs_bucket" AND resource.labels.bucket_name="{bucket_name}" AND '
            f'timestamp>="{timestamp.isoformat()}"'
        )

        logs = logging_client.list_entries(filter_=log_filter)

        # Construct BigQuery insert rows
        rows_to_insert = []
        for log_entry in logs:
            payload = log_entry.payload
            timestamp_str = log_entry.timestamp.isoformat()
            user = log_entry.principal_email
            method = payload.get("methodName")
            resource_name = payload.get("resourceName")
            if resource_name:
                object_name = resource_name.split("/")[-1]
            else:
                object_name = "N/A"

            row = {
                "timestamp": timestamp_str,
                "user": user,
                "method": method,
                "object_name": object_name,
                "log_entry": json.dumps(log_entry.to_api_repr())
            }
            rows_to_insert.append(row)

        if rows_to_insert:
            table_id = f"{project_id}.{bigquery_dataset}.{bigquery_table}"
            errors = bigquery_client.insert_rows_json(table_id, rows_to_insert)

            if errors:
                print(f"Errors occurred during BigQuery insert: {errors}")
            else:
                print(f"Successfully inserted {len(rows_to_insert)} log entries into BigQuery.")
        else:
            print("No log entries found matching the filter.")

    except Exception as e:
        print(f"Error: {e}")

import json

# Example usage:
project_id = "your-project-id"  # Replace with your project ID
bucket_name = "your-bucket-name"  # Replace with your bucket name
timestamp = datetime(2023, 10, 26, 12, 0, 0, tzinfo=pytz.utc)  # Replace with your desired timestamp
bigquery_dataset = "your_dataset"  # Replace with your BigQuery dataset
bigquery_table = "your_table"  # Replace with your BigQuery table

query_and_sink_gcs_logs(project_id, bucket_name, timestamp, bigquery_dataset, bigquery_table)
