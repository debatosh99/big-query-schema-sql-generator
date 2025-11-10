from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("BigLake Metastore Iceberg") \
    .config("spark.sql.catalog.learnbiglakeiceberg2", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.learnbiglakeiceberg2.catalog-impl", "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog") \
    .config("spark.sql.catalog.learnbiglakeiceberg2.gcp_project", "trim-strength-477307-h0") \
    .config("spark.sql.catalog.learnbiglakeiceberg2.gcp_location", "us-central1") \
    .config("spark.sql.catalog.learnbiglakeiceberg2.warehouse", "gs://learnbiglakeiceberg2/warehouse") \
    .getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS learnbiglakeiceberg2.iceberg_custom_catalog_namespace;")
spark.sql("USE learnbiglakeiceberg2.iceberg_custom_catalog_namespace;")
#spark.sql("CREATE TABLE TABLE_NAME (id int, data string) USING ICEBERG LOCATION 'WAREHOUSE_DIRECTORY';")


spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg_custom_catalog_namespace.person (
        id INT,
        name STRING,
        age INT
    ) USING iceberg
""")

# Insert sample data
data = [(1, "John", 30), (2, "Jane", 25), (3, "Bob", 35)]
df = spark.createDataFrame(data, ["id", "name", "age"])
df.writeTo("iceberg_custom_catalog_namespace.person").append()

# Query the table
print("Initial table contents:")
spark.sql("SELECT * FROM iceberg_custom_catalog_namespace.person").show()

# Add new column 'gender'
spark.sql("ALTER TABLE iceberg_custom_catalog_namespace.person ADD COLUMN gender STRING")

# Insert more data with gender
new_data = [(4, "Alice", 28, "F"), (5, "Charlie", 32, "M")]
new_df = spark.createDataFrame(new_data, ["id", "name", "age", "gender"])
new_df.writeTo("iceberg_custom_catalog_namespace.person").append()

# Query the updated table
print("\nUpdated table contents with gender:")
spark.sql("SELECT * FROM iceberg_custom_catalog_namespace.person").show()

# Describe the table
spark.sql("DESCRIBE iceberg_custom_catalog_namespace.person").show()

spark.stop()
