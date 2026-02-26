from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark Session
spark = SparkSession.builder.getOrCreate()

# Query the updated table
print("\nUpdated table contents with gender:")
spark.sql("SELECT * FROM iceberg_demo.person").show()

# Describe the table
spark.sql("DESCRIBE iceberg_demo.person").show()

new_data = [(400, "Ramu", 566, "F"), (500, "Shamu", 422, "M")]
new_df = spark.createDataFrame(new_data, ["id", "name", "age", "gender"])
new_df.writeTo("iceberg_demo.person").append()

# Stop Spark session
spark.stop()