from google.cloud import storage
import fastavro
import io

def extract_avro_schema_from_gcs(gcs_uri):
    """
    Extracts the Avro schema from a file stored in Google Cloud Storage.

    Args:
        gcs_uri: The GCS URI of the Avro file (e.g., gs://your-bucket/path/to/file.avro).

    Returns:
        The Avro schema as a Python dictionary, or None if an error occurs.
    """
    try:
        # 1. Download the Avro file from GCS
        storage_client = storage.Client()
        bucket_name, blob_name = gcs_uri.replace("gs://", "").split("/", 1)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        avro_data = blob.download_as_bytes()
        avro_file_like = io.BytesIO(avro_data)

        # 2. Read the Avro schema
        reader = fastavro.reader(avro_file_like)
        schema = reader.writer_schema

        return schema

    except Exception as e:
        print(f"Error extracting schema: {e}")
        return None

# Example usage:
gcs_avro_uri = "gs://your-bucket/path/to/your-file.avro"  # Replace with your GCS URI
avro_schema = extract_avro_schema_from_gcs(gcs_avro_uri)

if avro_schema:
    import json
    print(json.dumps(avro_schema, indent=2)) #print the schema in a readable format
