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

# Since we are using the same name for both custom catalog and rest catalog, but this cmd shows the tables custom catalog namespace
# spark.sql("SHOW TABLES IN learnbiglakeiceberg15.iceberg_custom_catalog_namespace15;").show()

#spark.sql("USE learnbiglakeiceberg16.iceberg_custom_catalog_namespace15;")
spark.sql("CREATE NAMESPACE IF NOT EXISTS learnbiglakeiceberg15.iceberg_custom_catalog_namespace15 LOCATION 'gs://learnbiglakeiceberg15/warehouse/iceberg_custom_catalog_namespace15'")

spark.sql("""
  CALL learnbiglakeiceberg15.system.register_table(
    table => 'iceberg_custom_catalog_namespace15.person1',
    metadata_file => 'gs://learnbiglakeiceberg15/warehouse/iceberg_custom_catalog_namespace15.db/person1/metadata/00003-edf69799-15d5-497d-9c1e-cdcff6d83cc0.metadata.json'
  )
""")

spark.sql("""
  CALL learnbiglakeiceberg15.system.register_table(
    table => 'iceberg_custom_catalog_namespace15.person2',
    metadata_file => 'gs://learnbiglakeiceberg15/warehouse/iceberg_custom_catalog_namespace15.db/person2/metadata/00003-10cb181d-400c-471c-bd2b-ca8f5d6de0bb.metadata.json'
  )
""")

spark.sql("SHOW TABLES IN learnbiglakeiceberg15.iceberg_custom_catalog_namespace15;").show()

spark.sql("USE learnbiglakeiceberg15.iceberg_custom_catalog_namespace15;")
spark.sql("SELECT * FROM iceberg_demo.person1").show()
spark.sql("SELECT * FROM iceberg_demo.person2").show()

spark.stop()