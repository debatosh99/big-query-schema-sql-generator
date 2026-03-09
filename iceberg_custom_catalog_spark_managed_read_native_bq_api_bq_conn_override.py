from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("BigLake Metastore Iceberg") \
    .getOrCreate()

spark.sql("USE learnbiglakeiceberg10.iceberg_demo;")

# Query the table
print("Initial table contents using native spark iceberg api:")
spark.sql("SELECT * FROM iceberg_demo.person_bq_override").show()

# Describe the table
spark.sql("DESCRIBE iceberg_demo.person_bq_override").show()


spark.conf.set("viewsEnabled", "true")
spark.conf.set("materializationDataset", "iceberg_demo")
spark.sql("USE NAMESPACE iceberg_demo;")
sql = """select * from iceberg_demo.person_bq_override"""

print("Initial table contents using native big query api:")
df = spark.read.format("bigquery").load(sql)
df.show()


spark.stop()