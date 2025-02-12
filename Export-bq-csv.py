from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# Define the table to export
table_id = "your_project_id.your_dataset.your_table"
table_ref = client.get_table(table_id)

# Define the destination URI in GCS
destination_uri = "gs://your-bucket/path/to/export/part_"

# Create a job configuration for the export
job_config = bigquery.ExtractJobConfig(
    destination_format="CSV",
    compression="NONE",
    field_delimiter=",",
    print_header=True,
    write_disposition="WRITE_TRUNCATE",
    destination_format_options={
        "csv": {
            "quote_all": False,
            "allow_quoted_newlines": True,
            "encoding": "UTF-8"
        }
    },
    max_bad_records=0,
    write_disposition="WRITE_EMPTY" 
)

# Start the export job
extract_job = client.extract_table(
    table_ref,
    destination_uri=destination_uri,
    job_config=job_config
)

# Wait for the job to complete
extract_job.result()

print(f"Exported {extract_job.output_rows} rows to {destination_uri}")
