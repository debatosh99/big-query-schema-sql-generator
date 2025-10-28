from google.cloud import bigquery

# ----- Configuration (replace with your values) -----
project_id = "your-gcp-project-id"
dataset_id = "your-bq-dataset-id"
table_id = "your-biglake-table-id"
connection_id = "your-connection-id"  # e.g., 'your-region.my-connection'
gcs_uris = ["gs://your-bucket-name/your-data-path/*.parquet"]

# Construct the full connection path
full_connection_id = f"projects/{project_id}/locations/us-central1/connections/{connection_id}"

# Initialize the BigQuery client
client = bigquery.Client(project=project_id)

def create_biglake_table():
    """Creates a BigLake table from existing GCS data."""
    
    # The SQL DDL statement to create the BigLake table
    create_table_sql = f"""
    CREATE EXTERNAL TABLE `{project_id}.{dataset_id}.{table_id}`
    WITH CONNECTION `{full_connection_id}`
    OPTIONS (
      format = 'PARQUET',
      uris = {gcs_uris}
    )
    """

    print(f"Creating BigLake table '{table_id}'...")
    try:
        query_job = client.query(create_table_sql)
        query_job.result()
        print(f"BigLake table '{table_id}' created successfully.")
    except Exception as e:
        print(f"Error creating BigLake table: {e}")

def query_biglake_table():
    """Queries the newly created BigLake table."""
    
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}` LIMIT 10"

    print("\nQuerying the BigLake table...")
    try:
        query_job = client.query(query)
        results = query_job.result()

        # Print query results
        for row in results:
            print(row)
    except Exception as e:
        print(f"Error querying BigLake table: {e}")

if __name__ == "__main__":
    create_biglake_table()
    query_biglake_table()
