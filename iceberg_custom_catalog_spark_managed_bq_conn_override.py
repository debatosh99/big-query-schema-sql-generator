from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("BigLake Metastore Iceberg") \
    .getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS learnbiglakeiceberg10.iceberg_demo;")
spark.sql("USE learnbiglakeiceberg10.iceberg_demo;")
#spark.sql("CREATE TABLE TABLE_NAME (id int, data string) USING ICEBERG LOCATION 'WAREHOUSE_DIRECTORY';")


spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg_demo.person_bq_override (
        id INT,
        name STRING,
        age INT
    ) USING iceberg
    LOCATION 'gs://learnbiglakeiceberg10/iceberg_demo/person_bq_override'
    TBLPROPERTIES ('bq_connection' = 'projects/trim-strength-477307-h0/locations/us-central1/connections/learnbiglakeiceberg10_bq_connection')
""")

# Insert sample data
data = [(1, "John", 30), (2, "Jane", 25), (3, "Bob", 35)]
df = spark.createDataFrame(data, ["id", "name", "age"])
df.writeTo("iceberg_demo.person_bq_override").append()

# Query the table
print("Initial table contents:")
spark.sql("SELECT * FROM iceberg_demo.person_bq_override").show()

# Describe the table
spark.sql("DESCRIBE iceberg_demo.person_bq_override").show()

spark.stop()