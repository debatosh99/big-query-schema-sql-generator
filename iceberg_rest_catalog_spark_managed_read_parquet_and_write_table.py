from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark Session
spark = SparkSession.builder.getOrCreate()

# Create namespace if not exists
spark.sql("USE learnbiglakeiceberg20.iceberg_rest_catalog_namespace20;")

###########################################################
parquet_file = "gs://dataproc_job_staging_bucket/customer_details_new.parquet"
df = spark.read.parquet(parquet_file)
df.createOrReplaceTempView("parquet_temp_view")

# Create Iceberg table
spark.sql("""
    CREATE TABLE IF NOT EXISTS customer_details_spark_managed 
    USING iceberg
    AS SELECT * FROM parquet_temp_view
""")

df.writeTo("customer_details_spark_managed").append()

spark.sql("SELECT * FROM customer_details_spark_managed").show()

spark.sql("DESCRIBE customer_details_spark_managed").show()

spark.stop()