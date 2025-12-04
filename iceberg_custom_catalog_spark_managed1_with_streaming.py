from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark Session (if running in a script, otherwise skip this in pyspark shell)
spark = SparkSession.builder.appName("StreamToBigQuery").getOrCreate()

# --- Configuration Variables ---
PROJECT_ID = "trim-strength-477307-h0"
DATASET_ID = "iceberg_custom_catalog_namespace"
TABLE_NAME = "person"
BIGQUERY_TABLE = f"{PROJECT_ID}:{DATASET_ID}.{TABLE_NAME}"
GCS_BUCKET = "your-gcs-bucket-name"
CHECKPOINT_LOCATION = f"gs://dataproc_job_staging_bucket/checkpoints/user_data_stream"
# -------------------------------

# Define the schema for the incoming sample data
# The 'rate' source provides 'timestamp' and 'value' columns by default.
stream_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Use the 'rate' source to generate a simple stream.
# We modify the generic 'value' to match our target schema structure.
# This generates a new row every second by default.
streaming_df = spark \
    .readStream \
    .format("rate") \
    .load() \
    .select(
        expr("value as id").cast(IntegerType()),
        expr("cast(uuid() as string) as name"), # Generate a random name string
        expr("cast(rand() * 100 as int) as age") # Generate a random age
    )


# Define the function to write each micro-batch to BigQuery
def write_to_bq_batch(batch_df, batch_id):
    # Use the 'direct' write method (Storage Write API) for better performance
    batch_df.write \
        .format("bigquery") \
        .option("table", BIGQUERY_TABLE) \
        .option("writeMethod", "direct") \
        .mode("append") \
        .save()

# Start the streaming query
print(f"Starting streaming query to write to {BIGQUERY_TABLE}...")

query = streaming_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_bq_batch) \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .start()

# Keep the stream running until terminated manually (e.g., Ctrl+C)
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Streaming query terminated by user.")
    query.stop()