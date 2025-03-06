from google.cloud import storage
import fastavro
import pyarrow as pa
import pyarrow.orc as pc
import io

def avro_to_orc_gcs(gcs_avro_uri, gcs_orc_uri):
    """
    Converts an Avro file in GCS to an ORC file and uploads it back to GCS.

    Args:
        gcs_avro_uri: The GCS URI of the Avro file (e.g., gs://bucket/path/file.avro).
        gcs_orc_uri: The GCS URI of the output ORC file (e.g., gs://bucket/path/file.orc).
    """
    try:
        # 1. Download Avro file from GCS
        storage_client = storage.Client()
        avro_bucket_name, avro_blob_name = gcs_avro_uri.replace("gs://", "").split("/", 1)
        avro_bucket = storage_client.bucket(avro_bucket_name)
        avro_blob = avro_bucket.blob(avro_blob_name)
        avro_data = avro_blob.download_as_bytes()
        avro_file_like = io.BytesIO(avro_data)

        # 2. Read Avro data and schema
        reader = fastavro.reader(avro_file_like)
        schema = reader.writer_schema
        records = list(reader)  # Load all records into memory. For large files, stream.

        # 3. Convert Avro records to PyArrow Table
        if not records:
            print("No Records found in the Avro file")
            return

        temp_buffer = io.BytesIO() #create a temp buffer to write to.
        fastavro.schemaless_writer(temp_buffer, schema, records[0]) #write first record to buffer.
        temp_buffer.seek(0) #reset the buffer.

        arrow_schema = fastavro.schemaless_reader(temp_buffer, schema) #read schema from buffer.
        arrow_table = pa.Table.from_pylist(records, schema=arrow_schema)

        # 4. Convert PyArrow Table to ORC format
        orc_buffer = io.BytesIO()
        pc.write_table(arrow_table, orc_buffer)
        orc_data = orc_buffer.getvalue()

        # 5. Upload ORC file to GCS
        orc_bucket_name, orc_blob_name = gcs_orc_uri.replace("gs://", "").split("/", 1)
        orc_bucket = storage_client.bucket(orc_bucket_name)
        orc_blob = orc_bucket.blob(orc_blob_name)
        orc_blob.upload_from_string(orc_data)

        print(f"Avro file '{gcs_avro_uri}' converted to ORC and uploaded to '{gcs_orc_uri}'")

    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage:
gcs_avro_uri = "gs://your-avro-bucket/your-avro-file.avro"
gcs_orc_uri = "gs://your-orc-bucket/your-orc-file.orc"

avro_to_orc_gcs(gcs_avro_uri, gcs_orc_uri)
