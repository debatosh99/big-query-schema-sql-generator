from google.cloud import storage
from google.cloud import logging
import json
from datetime import datetime, timedelta
import pytz

def get_bucket_activity_last_hour(bucket_name, project_id):
    """
    Retrieves write/read activity for a GCS bucket within the last hour,
    including user and timestamp.

    Args:
        bucket_name (str): The name of the GCS bucket.
        project_id (str): Your Google Cloud project ID.

    Returns:
        list: A list of dictionaries, each containing activity details, or None on error.
    """
    try:
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)

        logging_client = logging.Client(project=project_id)
        now_utc = datetime.now(pytz.utc)
        one_hour_ago_utc = now_utc - timedelta(hours=1)

        log_filter = (
            f'resource.type="gcs_bucket" AND resource.labels.bucket_name="{bucket_name}" AND '
            f'timestamp>="{one_hour_ago_utc.isoformat()}" AND '
            '(protoPayload.methodName="storage.objects.create" OR protoPayload.methodName="storage.objects.get")'
        )

        logs = logging_client.list_entries(filter_=log_filter, order="timestamp desc")

        activity_logs = []
        for log_entry in logs:
            payload = log_entry.payload
            method = payload.get("methodName")
            timestamp = log_entry.timestamp.isoformat()
            user = log_entry.principal_email

            resource_name = payload.get("resourceName")
            if resource_name:
                object_name = resource_name.split("/")[-1]
            else:
                object_name = "N/A"

            activity_logs.append({
                "timestamp": timestamp,
                "user": user,
                "method": method,
                "object_name": object_name,
            })

        return activity_logs

    except Exception as e:
        print(f"Error retrieving bucket activity: {e}")
        return None

# Example usage:
project_id = "your-project-id"  # Replace with your project ID
bucket_name = "your-bucket-name"  # Replace with your bucket name

activity_logs = get_bucket_activity_last_hour(bucket_name, project_id)

if activity_logs:
    print(json.dumps(activity_logs, indent=2))
