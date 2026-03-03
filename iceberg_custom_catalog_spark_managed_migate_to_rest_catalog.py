from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("BigLake Metastore Iceberg").getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS learnbiglakeiceberg15.iceberg_custom_catalog_namespace15;")
spark.sql("USE learnbiglakeiceberg15.iceberg_custom_catalog_namespace15;")
#spark.sql("CREATE TABLE TABLE_NAME (id int, data string) USING ICEBERG LOCATION 'WAREHOUSE_DIRECTORY';")


spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg_custom_catalog_namespace15.person1 (
        id INT,
        name STRING,
        age INT
    ) USING iceberg
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg_custom_catalog_namespace15.person2 (
        id INT,
        name STRING,
        age INT
    ) USING iceberg
""")

# Insert sample data
data = [(1, "John", 30), (2, "Jane", 25), (3, "Bob", 35)]
df = spark.createDataFrame(data, ["id", "name", "age"])
df.writeTo("iceberg_custom_catalog_namespace15.person1").append()
df.writeTo("iceberg_custom_catalog_namespace15.person2").append()

# Query the table
print("Initial table contents:")
spark.sql("SELECT * FROM iceberg_custom_catalog_namespace15.person1").show()
spark.sql("SELECT * FROM iceberg_custom_catalog_namespace15.person2").show()

# Add new column 'gender'
spark.sql("ALTER TABLE iceberg_custom_catalog_namespace15.person1 ADD COLUMN gender STRING")
spark.sql("ALTER TABLE iceberg_custom_catalog_namespace15.person2 ADD COLUMN gender STRING")


# Insert more data with gender
new_data = [(4, "Alice", 28, "F"), (5, "Charlie", 32, "M")]
new_df = spark.createDataFrame(new_data, ["id", "name", "age", "gender"])

new_df.writeTo("iceberg_custom_catalog_namespace15.person1").append()
new_df.writeTo("iceberg_custom_catalog_namespace15.person2").append()


# Query the updated table
print("\nUpdated table contents with gender:")
spark.sql("SELECT * FROM iceberg_custom_catalog_namespace15.person1").show()
spark.sql("SELECT * FROM iceberg_custom_catalog_namespace15.person2").show()


# Describe the table
spark.sql("DESCRIBE iceberg_custom_catalog_namespace15.person1").show()
spark.sql("DESCRIBE iceberg_custom_catalog_namespace15.person2").show()

spark.stop()
