from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark Session
spark = SparkSession.builder.getOrCreate()



# spark = SparkSession.builder \
#     .appName("BigLake Metastore Iceberg") \
#     .config("spark.sql.catalog.learnbiglakeiceberg11", "org.apache.iceberg.spark.SparkCatalog") \
#     .config("spark.sql.catalog.learnbiglakeiceberg11.type", "rest") \
#     .config("spark.sql.catalog.learnbiglakeiceberg11.uri", "https://biglake.googleapis.com/iceberg/v1/restcatalog") \
#     .config("warehouse","gs://learnbiglakeiceberg11") \
#     .config("spark.sql.catalog.learnbiglakeiceberg11.io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO") \
#     .config("spark.sql.catalog.learnbiglakeiceberg11.header.x-goog-user-project", "trim-strength-477307-h0") \
#     .config("spark.sql.catalog.learnbiglakeiceberg11.rest.auth.type", "org.apache.iceberg.gcp.auth.GoogleAuthManager") \
#     .config("spark.sql.catalog.learnbiglakeiceberg11.header.X-Iceberg-Access-Delegation", "vended-credentials") \
#     .config("spark.sql.catalog.learnbiglakeiceberg11.rest-metrics-reporting-enabled", "false") \
#     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
#     .getOrCreate()



# Create namespace if not exists
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_demo")
#spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_demo LOCATION 'gs://learnbiglakeiceberg1'")

###########################################################

# Create table schema
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("age", IntegerType(), False)
])

# Create Iceberg table
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg_demo.person (
        id INT,
        name STRING,
        age INT
    ) USING iceberg
""")

# #######################################################################
# # Insert sample data
# data = [(1, "John", 30), (2, "Jane", 25), (3, "Bob", 35)]
# df = spark.createDataFrame(data, ["id", "name", "age"])
# df.writeTo("iceberg_demo.person").append()
#
# # Query the table
# print("Initial table contents:")
# spark.sql("SELECT * FROM iceberg_demo.person").show()
#
# # Add new column 'gender'
# spark.sql("ALTER TABLE iceberg_demo.person ADD COLUMN gender STRING")
# #######################################################################


# Insert more data with gender
new_data = [(40, "Alicei", 280, "F"), (50, "Charliei", 321, "M")]
new_df = spark.createDataFrame(new_data, ["id", "name", "age", "gender"])
new_df.writeTo("iceberg_demo.person").append()

# Query the updated table
print("\nUpdated table contents with gender:")
spark.sql("SELECT * FROM iceberg_demo.person").show()

# Describe the table
spark.sql("DESCRIBE iceberg_demo.person").show()

#########################################################

# Insert more data with gender
new_data = [(40, "Aliceyy", 566, "F"), (50, "Charlieyy", 422, "M")]
new_df = spark.createDataFrame(new_data, ["id", "name", "age", "gender"])
new_df.writeTo("iceberg_demo.person").append()

# Stop Spark session
spark.stop()