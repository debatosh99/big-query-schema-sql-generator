from google.cloud import bigquery
import os
import json

#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"
#os.environ["GCLOUD_PROJECT"] = "playground-s-11-42147a98"
#os.environ["REQUESTS_CA_BUNDLE"] = "<>"

# Construct a BigQuery client
client = bigquery.Client()

# Specify the project ID, dataset name, and table name
project_id = "playground-s-11-42147a98"
dataset_name = "test_dataset"
table_name = "student_records"

# Construct a fully qualified table name
table_id = f"{project_id}.{dataset_name}.{table_name}"

# Get the table schema
table = client.get_table(table_id)
schema = table.schema

def _convert_field(field):
  field_dict = {
    "name": field.name,
    "type": field.field_type,
    "mode": field.mode
  }
  if field.fields:
    field_dict["fields"] = [_convert_field(f) for f in field.fields]
  return field_dict

# Convert the schema to JSON format
schema_json = [_convert_field(field) for field in schema]

# Print the JSON schema
print(json.dumps(schema_json, indent=4))
