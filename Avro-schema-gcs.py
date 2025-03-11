from google.cloud import storage
import fastavro
import json
import io

def download_avro_file_from_gcs(bucket_name, source_blob_name):
    """Downloads an Avro file from GCS and returns its content as bytes."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    return blob.download_as_bytes()

def extract_avro_schema(avro_bytes):
    """Extracts the Avro schema from an Avro file's bytes."""
    avro_file_like = io.BytesIO(avro_bytes)
    reader = fastavro.reader(avro_file_like)
    return reader.schema

def upload_json_to_gcs(bucket_name, destination_blob_name, json_data):
    """Uploads a JSON file to GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(json.dumps(json_data, indent=2), content_type="application/json")
    print(f"Schema JSON uploaded to gs://{bucket_name}/{destination_blob_name}")

def extract_and_upload_avro_schema(bucket_name, source_blob_name, destination_blob_name):
    """Extracts the Avro schema from a file in GCS and uploads it as a JSON file to GCS."""
    # Download the Avro file from GCS
    avro_bytes = download_avro_file_from_gcs(bucket_name, source_blob_name)
    
    # Extract the Avro schema
    schema = extract_avro_schema(avro_bytes)
    
    # Upload the schema as a JSON file to GCS
    upload_json_to_gcs(bucket_name, destination_blob_name, schema)

# Example usage
if __name__ == "__main__":
    bucket_name = "your-gcs-bucket-name"
    source_blob_name = "path/to/your/avro/file.avro"
    destination_blob_name = "path/to/save/schema.json"
    
    extract_and_upload_avro_schema(bucket_name, source_blob_name, destination_blob_name)
