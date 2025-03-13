import pandas as pd
from google.cloud import logging
from sqlalchemy import create_engine
from datetime import datetime, timedelta

#pip install google-cloud-logging sqlalchemy psycopg2-binary pandas
# Google Cloud project and bucket details
project_id = 'your-project-id'
bucket_name = 'your-bucket-name'

# Cloud SQL PostgreSQL credentials
db_user = 'your-db-user'
db_pass = 'your-db-pass'
db_name = 'your-db-name'
db_host = 'your-db-host'
db_port = '5432'

# Initialize the Cloud Logging client
client = logging.Client(project=project_id)

# Define the log filter for GCS access logs
log_filter = f"""
resource.type="gcs_bucket"
resource.labels.bucket_name="{bucket_name}"
logName="projects/{project_id}/logs/cloudaudit.googleapis.com%2Fdata_access"
"""

# Query Cloud Logging for GCS access logs
logger = client.logger('cloudaudit.googleapis.com%2Fdata_access')
entries = client.list_entries(filter_=log_filter, page_size=1000)

# Process the log entries
data = []
for entry in entries:
    payload = entry.payload
    if 'protoPayload' in payload:
        proto_payload = payload['protoPayload']
        timestamp = entry.timestamp.isoformat()
        method_name = proto_payload.get('methodName', 'N/A')
        principal_email = proto_payload.get('authenticationInfo', {}).get('principalEmail', 'N/A')
        resource_name = proto_payload.get('resourceName', 'N/A')
        data.append([timestamp, method_name, resource_name, principal_email])

# Create a DataFrame
df = pd.DataFrame(data, columns=['timestamp', 'method_name', 'resource_name', 'principal_email'])

# Create SQLAlchemy engine for Cloud SQL PostgreSQL
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

# Write DataFrame to PostgreSQL table
df.to_sql('gcs_access_logs', engine, if_exists='append', index=False)

print("Data successfully written to Cloud SQL PostgreSQL.")
