from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark Session
spark = SparkSession.builder.getOrCreate()

spark.sql("USE learnbiglakeiceberg4.customer")

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

# Insert sample data not possible even though we used big query catalog federation
# and use iceberg rest catalog on big query managed iceberg table which was originally
# created in BQ and always metadata has to be exported inorder for iceberg custom catalog in blms to be used in spark to query read data

data = [(10, "Johny", 30), (20, "Janey", 25), (30, "Boby", 35)]
df = spark.createDataFrame(data, ["id", "name", "age"])
#df.writeTo("customer.customer_details_iceberg_bq_managed").append()

print("\nTable contents:")
spark.sql("SELECT * FROM customer.customer_details_iceberg_bq_managed").show()

# Stop Spark session
spark.stop()