from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark Session
spark = SparkSession.builder.getOrCreate()


# Create namespace if not exists
# spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_demo2")
#spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_demo2 LOCATION 'gs://learnbiglakeiceberg18'")

###########################################################

# # Create table schema
# schema = StructType([
#     StructField("id", IntegerType(), False),
#     StructField("name", StringType(), False),
#     StructField("age", IntegerType(), False)
# ])
#
# # Create Iceberg table
# spark.sql("""
#     CREATE TABLE IF NOT EXISTS iceberg_demo2.person2 (
#         id INT,
#         name STRING,
#         age INT
#     ) USING iceberg
# """)

# #######################################################################
# # Insert sample data
# data = [(1, "John", 30), (2, "Jane", 25), (3, "Bob", 35)]
# df = spark.createDataFrame(data, ["id", "name", "age"])
# df.writeTo("iceberg_demo2.person2").append()
#
# # Query the table
# print("Initial table contents:")
# spark.sql("SELECT * FROM iceberg_demo2.person2").show()
#
# # Add new column 'gender'
# spark.sql("ALTER TABLE iceberg_demo2.person2 ADD COLUMN gender STRING")
# #######################################################################


# # Insert more data with gender
# new_data = [(40, "Alicei", 280, "F"), (50, "Charliei", 321, "M")]
# new_df = spark.createDataFrame(new_data, ["id", "name", "age", "gender"])
# new_df.writeTo("iceberg_demo2.person2").append()

# Query the updated table
# print("\nUpdated table contents with gender:")
spark.sql("SELECT * FROM iceberg_demo2.person2").show()

# Describe the table
spark.sql("DESCRIBE iceberg_demo2.person2").show()

#########################################################

# Insert more data with gender
# new_data = [(40, "Aliceyy", 566, "F"), (50, "Charlieyy", 422, "M")]
# new_df = spark.createDataFrame(new_data, ["id", "name", "age", "gender"])
# new_df.writeTo("iceberg_demo2.person2").append()

# Stop Spark session
spark.stop()