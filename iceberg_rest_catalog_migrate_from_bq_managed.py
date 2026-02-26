from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys

spark = SparkSession.builder.appName("BigLake Metastore Iceberg").getOrCreate()

try:
    # Test catalog connectivity first
    spark.sql("SHOW CATALOGS").show()
    print("Successfully connected to catalogs", file=sys.stderr)
except Exception as e:
    print(f"Error connecting to catalogs: {e}", file=sys.stderr)
    spark.stop()
    sys.exit(1)


spark.sql("CREATE NAMESPACE IF NOT EXISTS learnbiglakeiceberg20.iceberg_rest_catalog_namespace20 LOCATION 'gs://learnbiglakeiceberg20/iceberg_rest_catalog_namespace20'")

# spark.sql("SHOW TABLES IN learnbiglakeiceberg15.iceberg_custom_catalog_namespace15;").show()
spark.sql("SHOW TABLES IN learnbiglakeiceberg20.iceberg_rest_catalog_namespace20;").show()

spark.sql("""
  CALL learnbiglakeiceberg20.system.register_table(
    table => 'iceberg_rest_catalog_namespace20.customer_details_bq_managed',
    metadata_file => 'gs://learnbiglakeiceberg20/iceberg_rest_catalog_namespace20/customer_details_bq_managed/metadata/00000-699c85e3-0000-252a-a12f-d4f547fdaaa4.metadata.json'
  )
""")

spark.sql("SHOW TABLES IN learnbiglakeiceberg20.iceberg_rest_catalog_namespace20;").show()


spark.sql("USE learnbiglakeiceberg20.iceberg_rest_catalog_namespace20;")

spark.sql("SELECT * FROM learnbiglakeiceberg20.iceberg_rest_catalog_namespace20.customer_details_bq_managed").show()

spark.stop()