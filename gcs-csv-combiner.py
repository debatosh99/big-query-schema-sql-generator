import pandas as pd
from google.cloud import storage

# Initialize a GCS client
client = storage.Client()

# GCS bucket name
bucket_name = 'your-bucket-name'

# Prefix (folder path) in GCS where the partitioned CSV files are stored
prefix = 'path/to/your/partitioned/csv/files/'

# Output file path for the combined CSV (can be local or in GCS)
output_file = 'path/to/output/combined_file.csv'  # Can be local or GCS path

# Get the bucket object
bucket = client.bucket(bucket_name)

# List all blobs (files) in the specified prefix (folder)
blobs = bucket.list_blobs(prefix=prefix)

# Initialize a flag to check if the header has been written
header_written = False

# Open the output file for writing
if output_file.startswith('gs://'):
    # If the output path is in GCS
    output_bucket_name, output_blob_name = output_file[5:].split('/', 1)
    output_bucket = client.bucket(output_bucket_name)
    output_blob = output_bucket.blob(output_blob_name)
    output_buffer = output_blob.open('w')
else:
    # If the output path is local
    output_buffer = open(output_file, 'w')

# Process each blob (file) in GCS
for blob in blobs:
    if blob.name.endswith('.csv'):
        print(f"Processing file: {blob.name}")
        
        # Download the CSV file as a stream
        with blob.open("rt") as csv_file:
            # Read the CSV file in chunks
            for chunk in pd.read_csv(csv_file, chunksize=100000):  # Adjust chunksize as needed
                # Write the header only once
                if not header_written:
                    chunk.to_csv(output_buffer, index=False, header=True)
                    header_written = True
                else:
                    chunk.to_csv(output_buffer, index=False, header=False)

# Close the output buffer
output_buffer.close()

print(f"Combined CSV file saved to: {output_file}")