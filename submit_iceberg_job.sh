#!/bin/bash

gcloud config set account nitipradhan17@gmail.com
gcloud auth login

###############################################################################################
ICEBERG REST CATALOG
###############################################################################################
# Replace these variables with your values
PYSPARK_FILE="gs://learnbiglakeicerg-artifacts/iceberg_rest_catalog_spark_managed1.py"
PROJECT_ID="trim-strength-477307-h0"
REGION="us-central1"
RUNTIME_VERSION="2.2"
CATALOG_NAME="learnbiglakeiceberg1"
WAREHOUSE_PATH="gs://learnbiglakeiceberg1"
STAGE_BUCKET_PATH="gs://dataproc_job_staging_bucket"


# Submit the Spark job
gcloud dataproc batches submit pyspark ${PYSPARK_FILE} \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --version=${RUNTIME_VERSION} \
    --deps-bucket=${STAGE_BUCKET_PATH} \
    --properties="\
spark.sql.defaultCatalog=${CATALOG_NAME},\
spark.sql.catalog.${CATALOG_NAME}=org.apache.iceberg.spark.SparkCatalog,\
spark.sql.catalog.${CATALOG_NAME}.type=rest,\
spark.sql.catalog.${CATALOG_NAME}.uri=https://biglake.googleapis.com/iceberg/v1/restcatalog,\
spark.sql.catalog.${CATALOG_NAME}.warehouse=${WAREHOUSE_PATH},\
spark.sql.catalog.${CATALOG_NAME}.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO,\
spark.sql.catalog.${CATALOG_NAME}.header.x-goog-user-project=${PROJECT_ID},\
spark.sql.catalog.${CATALOG_NAME}.rest.auth.type=org.apache.iceberg.gcp.auth.GoogleAuthManager,\
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,\
spark.sql.catalog.${CATALOG_NAME}.rest-metrics-reporting-enabled=false"

# Query in Bigquery and no insert possible
select * from `trim-strength-477307-h0.learnbiglakeiceberg1.iceberg_demo.person`;

###############################################################################################
ICEBERG CUSTOM CATALOG (Spark managed iceberg)
###############################################################################################
PYSPARK_FILE="gs://learnbiglakeicerg-artifacts/iceberg_custom_catalog_spark_managed1.py"
PROJECT_ID="trim-strength-477307-h0"
REGION="us-central1"
LOCATION="us-central1"
RUNTIME_VERSION="2.2"
CATALOG_NAME="learnbiglakeiceberg2"
WAREHOUSE_DIRECTORY="gs://learnbiglakeiceberg2/warehouse"
STAGE_BUCKET_PATH="gs://dataproc_job_staging_bucket"

gcloud dataproc batches submit pyspark ${PYSPARK_FILE} \
    --version=2.2 \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --deps-bucket=${STAGE_BUCKET_PATH} \
    --properties="\
    spark.sql.catalog.${CATALOG_NAME}=org.apache.iceberg.spark.SparkCatalog,\
    spark.sql.catalog.${CATALOG_NAME}.catalog-impl=org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog,\
    spark.sql.catalog.${CATALOG_NAME}.gcp_project=${PROJECT_ID},\
    spark.sql.catalog.${CATALOG_NAME}.gcp_location=${LOCATION},\
    spark.sql.catalog.${CATALOG_NAME}.warehouse=${WAREHOUSE_DIRECTORY}"

# Query in Bigquery and no insert possible
select * from `trim-strength-477307-h0.iceberg_custom_catalog_namespace.person`;



###############################################################################################
ICEBERG CUSTOM CATALOG (BigQuery managed iceberg table)
###############################################################################################
PYSPARK_FILE="gs://learnbiglakeicerg-artifacts/iceberg_custom_catalog_bq_managed1.py"
PROJECT_ID="trim-strength-477307-h0"
REGION="us-central1"
LOCATION="us-central1"
RUNTIME_VERSION="2.2"
CATALOG_NAME="learnbiglakeiceberg3"
WAREHOUSE_DIRECTORY="gs://learnbiglakeiceberg3"
STAGE_BUCKET_PATH="gs://dataproc_job_staging_bucket"

gcloud dataproc batches submit pyspark ${PYSPARK_FILE} \
  --version=2.2 \
  --project=${PROJECT_ID} \
  --region=${REGION} \
  --deps-bucket=${STAGE_BUCKET_PATH} \
  --properties="\
    spark.driver.cores=1,\
    spark.driver.memory=1g,\
    spark.executor.cores=1,\
    spark.executor.memory=1g,\
    spark.executor.instances=1,\
    spark.dynamicAllocation.enabled=false,\
    spark.sql.catalog.${CATALOG_NAME}=org.apache.iceberg.spark.SparkCatalog,\
    spark.sql.catalog.${CATALOG_NAME}.catalog-impl=org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog,\
    spark.sql.catalog.${CATALOG_NAME}.gcp_project=${PROJECT_ID},\
    spark.sql.catalog.${CATALOG_NAME}.gcp_location=${LOCATION},\
    spark.sql.catalog.${CATALOG_NAME}.warehouse=${WAREHOUSE_DIRECTORY}"

###########################################################################################
ICEBERG EXTERNAL TABLE NO BLMS
###########################################################################################
  CREATE EXTERNAL TABLE `trim-strength-477307-h0.customer.customer_details_external_no_blms`
  WITH CONNECTION `trim-strength-477307-h0.us-central1.learnbiglakeiceberg3_bq_connection`
  OPTIONS (
         format = 'ICEBERG',
         uris = ["gs://learnbiglakeiceberg3/metadata/v1762638041.metadata.json"]
   );

select * from `trim-strength-477307-h0.customer.customer_details_external_no_blms`;

Note:  Replace the uris value with the latest JSON metadata file for a specific table snapshot.


##########################################################################################
ICEBERG REST CATALOG WITH BQ CATALOG FEDERATION
##########################################################################################
PYSPARK_FILE="gs://learnbiglakeicerg-artifacts/iceberg_rest_catalog_bq_catalog_federation.py"
PROJECT_ID="trim-strength-477307-h0"
REGION="us-central1"
RUNTIME_VERSION="2.2"
CATALOG_NAME="learnbiglakeiceberg4"
WAREHOUSE_PATH="bq://projects/trim-strength-477307-h0"
STAGE_BUCKET_PATH="gs://dataproc_job_staging_bucket"


# Submit the Spark job
gcloud dataproc batches submit pyspark ${PYSPARK_FILE} \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --version=${RUNTIME_VERSION} \
    --deps-bucket=${STAGE_BUCKET_PATH} \
    --properties="\
spark.sql.defaultCatalog=${CATALOG_NAME},\
spark.sql.catalog.${CATALOG_NAME}=org.apache.iceberg.spark.SparkCatalog,\
spark.sql.catalog.${CATALOG_NAME}.type=rest,\
spark.sql.catalog.${CATALOG_NAME}.uri=https://biglake.googleapis.com/iceberg/v1/restcatalog,\
spark.sql.catalog.${CATALOG_NAME}.warehouse=${WAREHOUSE_PATH},\
spark.sql.catalog.${CATALOG_NAME}.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO,\
spark.sql.catalog.${CATALOG_NAME}.header.x-goog-user-project=${PROJECT_ID},\
spark.sql.catalog.${CATALOG_NAME}.rest.auth.type=org.apache.iceberg.gcp.auth.GoogleAuthManager,\
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,\
spark.sql.catalog.${CATALOG_NAME}.rest-metrics-reporting-enabled=false"

# Query in Bigquery
select * from `trim-strength-477307-h0.customer.customer_details_iceberg_bq_managed`;


##########################################################################################
ICEBERG REST CATALOG WITHOUT USING DEFAULT REST CATALOG BUCKET  (WORKS)
##########################################################################################
PYSPARK_FILE="gs://learnbiglakeicerg-artifacts/iceberg_rest_catalog_spark_managed_without_default_cat_dir.py"
PROJECT_ID="trim-strength-477307-h0"
REGION="us-central1"
LOCATION="us-central1"
RUNTIME_VERSION="2.2"
CATALOG_NAME="learnbiglakeiceberg7"
WAREHOUSE_DIRECTORY="gs://learnbiglakeiceberg7"
STAGE_BUCKET_PATH="gs://dataproc_job_staging_bucket"

gcloud dataproc batches submit pyspark ${PYSPARK_FILE} \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --version=${RUNTIME_VERSION} \
    --deps-bucket=${STAGE_BUCKET_PATH} \
    --properties="\
spark.sql.defaultCatalog=${CATALOG_NAME},\
spark.sql.catalog.${CATALOG_NAME}=org.apache.iceberg.spark.SparkCatalog,\
spark.sql.catalog.${CATALOG_NAME}.type=rest,\
spark.sql.catalog.${CATALOG_NAME}.uri=https://biglake.googleapis.com/iceberg/v1/restcatalog,\
spark.sql.catalog.${CATALOG_NAME}.warehouse=${WAREHOUSE_DIRECTORY},\
spark.sql.catalog.${CATALOG_NAME}.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO,\
spark.sql.catalog.${CATALOG_NAME}.header.x-goog-user-project=${PROJECT_ID},\
spark.sql.catalog.${CATALOG_NAME}.rest.auth.type=org.apache.iceberg.gcp.auth.GoogleAuthManager,\
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,\
spark.sql.catalog.${CATALOG_NAME}.rest-metrics-reporting-enabled=false"


Note: In this type of setup you always do need to specify the location of the namespace which should be always inside your catalog gcs directory
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_rest_cat_no_default_cat LOCATION 'gs://learnbiglakeiceberg7/mynamespace7'")

select * from `trim-strength-477307-h0.learnbiglakeiceberg7.iceberg_rest_cat_no_default_cat.person`;

#########################################################################################
