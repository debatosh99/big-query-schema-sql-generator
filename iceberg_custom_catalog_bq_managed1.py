from pyspark.sql import SparkSession

# Create a spark session
spark = SparkSession.builder \
.appName("BigLake Metastore Iceberg") \
.config("spark.sql.catalog.learnbiglakeiceberg3", "org.apache.iceberg.spark.SparkCatalog") \
.config("spark.sql.catalog.learnbiglakeiceberg3.catalog-impl", "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog") \
.config("spark.sql.catalog.learnbiglakeiceberg3.gcp_project", "trim-strength-477307-h0") \
.config("spark.sql.catalog.learnbiglakeiceberg3.gcp_location", "us-central1") \
.config("spark.sql.catalog.learnbiglakeiceberg3.warehouse", "gs://learnbiglakeiceberg3") \
.getOrCreate()
spark.conf.set("viewsEnabled","true")

# Use the blms_catalog
#spark.sql("USE `CATALOG_NAME`;")
#spark.sql("USE `learnbiglakeiceberg3`")
#spark.sql("USE NAMESPACE DATASET_NAME;")
# Use the blms_catalog
#spark.sql("USE `CATALOG_NAME`;")
#spark.sql("USE `learnbiglakeiceberg3`")
#spark.sql("USE NAMESPACE DATASET_NAME;")
spark.sql("USE learnbiglakeiceberg3.customer")

# List the tables in the dataset
df = spark.sql("SHOW TABLES;")
df.show()

# Query the tables using fully qualified BigQuery table name
#table_path = "trim-strength-477307-h0.customer.customer_details_iceberg_bq_managed"
#df = spark.read.format("bigquery").option("table", table_path).load()
#df.show()

# Show table history
print("\nTable contents:")
spark.sql("SELECT * FROM customer.customer_details_iceberg_bq_managed").show()

# Show table history
print("\nTable History:")
spark.sql("SELECT * FROM customer.customer_details_iceberg_bq_managed.history").show()

# Show table snapshots
print("\nTable Snapshots:")
spark.sql("SELECT * FROM customer.customer_details_iceberg_bq_managed.snapshots").show()

# Show detailed manifest information
#print("\nManifest Details:")
#spark.sql("SELECT * FROM customer.customer_details_iceberg_bq_managed.manifests").show()

# Show files in the table
#print("\nTable Files:")
#spark.sql("SELECT * FROM customer.customer_details_iceberg_bq_managed.files").show()

# Show table metadata and properties
print("\nTable Metadata:")
spark.sql("DESCRIBE TABLE EXTENDED customer.customer_details_iceberg_bq_managed").show(truncate=False)

# Example of time travel query (as of timestamp)
print("\nTime Travel Query (example):")
spark.sql("""
    SELECT * FROM customer.customer_details_iceberg_bq_managed TIMESTAMP AS OF 
    (SELECT timestamp FROM customer.customer_details_iceberg_bq_managed.history ORDER BY timestamp DESC LIMIT 1)
""").show()

# Query by snapshot ID (replace SNAPSHOT_ID with actual ID from snapshots table)
#print("\nQuery by Snapshot ID:")
#spark.sql("""
#    SELECT * FROM customer.customer_details_iceberg_bq_managed VERSION AS OF <SNAPSHOT_ID>
#""").show()

# Incremental queries using watermarks
print("\nIncremental Query Example:")
spark.sql("""
    SELECT * FROM customer.customer_details_iceberg_bq_managed 
    WHERE _file_modified_time > TIMESTAMP '2025-01-01 00:00:00'
""").show()

# Show partition evolution
#print("\nPartition Evolution:")
#spark.sql("SELECT * FROM customer.customer_details_iceberg_bq_managed.partition_data").show()

# Show table references (tags and branches)
#print("\nTable References:")
#spark.sql("SELECT * FROM customer.customer_details_iceberg_bq_managed.refs").show()

# Show all table metadata properties
print("\nTable Properties:")
spark.sql("SHOW TBLPROPERTIES customer.customer_details_iceberg_bq_managed").show()

# Example of time travel query using for system_time
print("\nTime Travel Using FOR SYSTEM_TIME AS OF:")
spark.sql("""
    SELECT * FROM customer.customer_details_iceberg_bq_managed FOR SYSTEM_TIME AS OF 
    TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
""").show()

spark.stop()
