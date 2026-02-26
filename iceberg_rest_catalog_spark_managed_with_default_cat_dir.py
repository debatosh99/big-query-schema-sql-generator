from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark Session
spark = SparkSession.builder.getOrCreate()

# Create namespace if not exists
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_rest_cat_with_default_dir_namespace")
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
    CREATE TABLE IF NOT EXISTS iceberg_rest_cat_with_default_dir_namespace.person (
        id INT,
        name STRING,
        age INT
    ) USING iceberg
""")

# Insert sample data
data = [(1, "John", 30), (2, "Jane", 25), (3, "Bob", 35)]
df = spark.createDataFrame(data, ["id", "name", "age"])
df.writeTo("iceberg_rest_cat_with_default_dir_namespace.person").append()

# Query the table
print("Initial table contents:")
spark.sql("SELECT * FROM iceberg_rest_cat_with_default_dir_namespace.person").show()

# Add new column 'gender'
spark.sql("ALTER TABLE iceberg_rest_cat_with_default_dir_namespace.person ADD COLUMN gender STRING")

# Insert more data with gender
new_data = [(4, "Alice", 28, "F"), (5, "Charlie", 32, "M")]
new_df = spark.createDataFrame(new_data, ["id", "name", "age", "gender"])
new_df.writeTo("iceberg_rest_cat_with_default_dir_namespace.person").append()

# Query the updated table
print("\nUpdated table contents with gender:")
spark.sql("SELECT * FROM iceberg_rest_cat_with_default_dir_namespace.person").show()

# Describe the table
spark.sql("DESCRIBE iceberg_rest_cat_with_default_dir_namespace.person").show()

#########################################################

# Insert more data with gender
new_data = [(40, "Alicey", 56, "F"), (50, "Charliey", 42, "M")]
new_df = spark.createDataFrame(new_data, ["id", "name", "age", "gender"])
new_df.writeTo("iceberg_rest_cat_with_default_dir_namespace.person").append()

# Stop Spark session
spark.stop()