from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("BigLake Metastore Iceberg") \
    .getOrCreate()

#spark.sql("CREATE NAMESPACE IF NOT EXISTS learnbiglakeiceberg21.iceberg_ds_ns LOCATION 'gs://learnbiglakeiceberg21/iceberg_ds_ns' WITH DBPROPERTIES ('gcp-region' = 'us-central1');")
spark.sql("USE learnbiglakeiceberg21.iceberg_ds_ns;")
#spark.sql("CREATE TABLE TABLE_NAME (id int, data string) USING ICEBERG LOCATION 'WAREHOUSE_DIRECTORY';")


spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg_ds_ns.person (
        id INT,
        name STRING,
        age INT
    ) USING iceberg
    LOCATION 'gs://learnbiglakeiceberg21/iceberg_ds_ns/person'
""")

# Insert sample data
data = [(4, "Johny", 30), (5, "Janey", 25), (6, "Boby", 35)]
df = spark.createDataFrame(data, ["id", "name", "age"])
df.writeTo("iceberg_ds_ns.person").append()

# Query the table
print("Initial table contents:")
spark.sql("SELECT * FROM iceberg_ds_ns.person").show()

# Describe the table
spark.sql("DESCRIBE iceberg_ds_ns.person").show()

spark.stop()