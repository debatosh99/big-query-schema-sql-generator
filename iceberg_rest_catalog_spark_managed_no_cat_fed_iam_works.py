from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark Session
spark = SparkSession.builder.getOrCreate()



#spark.sql("CREATE NAMESPACE IF NOT EXISTS learnbiglakeiceberg18.iceberg_demo_now;")
spark.sql("CREATE NAMESPACE IF NOT EXISTS learnbiglakeiceberg18.iceberg_demo_now_nocat;")

spark.sql("USE learnbiglakeiceberg18.iceberg_demo_now_nocat;")
#spark.sql("CREATE TABLE TABLE_NAME (id int, data string) USING ICEBERG LOCATION 'WAREHOUSE_DIRECTORY';")

print("----------SHOW NAMESPACES IN learnbiglakeiceberg18------------")
spark.sql("SHOW NAMESPACES IN learnbiglakeiceberg18;").show()

spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg_demo_now_nocat.person_now_nocat (
        id INT,
        name STRING,
        age INT
    ) USING iceberg
    LOCATION 'gs://learnbiglakeiceberg18/iceberg_demo_now_nocat/person_now_nocat'
""")


print("----SHOW TABLES----")
spark.sql("SHOW TABLES;").show()

# Show table history
print("\nTable contents:")
spark.sql("SELECT * FROM iceberg_demo_now_nocat.person_now_nocat").show()

# Show table history
print("\nTable History:")
spark.sql("SELECT * FROM iceberg_demo_now_nocat.person_now_nocat.history").show()

# Show table snapshots
print("\nTable Snapshots:")
spark.sql("SELECT * FROM iceberg_demo_now_nocat.person_now_nocat.snapshots").show()

# Insert sample data not possible even though we used big query catalog federation
# and use iceberg rest catalog on big query managed iceberg table which was originally
# created in BQ and always metadata has to be exported inorder for iceberg custom catalog in blms to be used in spark to query read data

data = [(10, "Johny", 30), (20, "Janey", 25), (30, "Boby", 35)]
df = spark.createDataFrame(data, ["id", "name", "age"])
df.writeTo("iceberg_demo_now_nocat.person_now_nocat").append()

print("\nTable contents:")
spark.sql("SELECT * FROM iceberg_demo_now_nocat.person_now_nocat").show()

# Stop Spark session
spark.stop()