#!/bin/bash

gcloud config set account nitipradhan17@gmail.com
gcloud auth login

##############################################################################################
##############################################################################################
ICEBERG REST CATALOG (with catalog federation to be able to see the namespace - created as dataset in the BQ studio)
##############################################################################################
##############################################################################################
# Replace these variables with your values
PYSPARK_FILE="gs://learnbiglakeicerg-artifacts/iceberg_rest_catalog_spark_managed1_cat_fed_bq_studio_visible.py"
PROJECT_ID="trim-strength-477307-h0"
REGION="us-central1"
RUNTIME_VERSION="2.2"
CATALOG_NAME="learnbiglakeiceberg1"
STAGE_BUCKET_PATH="gs://dataproc_job_staging_bucket"
WAREHOUSE_PATH="bq://projects/trim-strength-477307-h0"

WAREHOUSE_PATH="bq://projects/trim-strength-477307-h0/locations/us-central1"


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
Note: The dataset "iceberg_demo" is not visible in big query studio
select * from `trim-strength-477307-h0.learnbiglakeiceberg5.iceberg_demo5.person`;


$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
PYSPARK_FILE="gs://learnbiglakeicerg-artifacts/iceberg_rest_catalog_spark_managed1_cat_fed_bq_studio_visible.py"
PROJECT_ID="trim-strength-477307-h0"
REGION="us-central1"
RUNTIME_VERSION="2.2"
CATALOG_NAME="learnbiglakeiceberg5"
STAGE_BUCKET_PATH="gs://dataproc_job_staging_bucket"
WAREHOUSE_PATH="gs://learnbiglakeiceberg5"


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
$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

###############################################################################################

###############################################################################################
ICEBERG REST CATALOG (spark managed)
###############################################################################################
# Replace these variables with your values
PYSPARK_FILE="gs://learnbiglakeicerg-artifacts/iceberg_rest_catalog_spark_managed1.py"
PROJECT_ID="trim-strength-477307-h0"
REGION="us-central1"
RUNTIME_VERSION="2.2"
CATALOG_NAME="learnbiglakeiceberg1"
STAGE_BUCKET_PATH="gs://dataproc_job_staging_bucket"
WAREHOUSE_PATH="gs://learnbiglakeiceberg1"


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
Note: The dataset "iceberg_demo" is not visible in big query studio
select * from `trim-strength-477307-h0.learnbiglakeiceberg1.iceberg_demo.person`;

#Time travel is possible for spark managed iceberg (rest catalog)
select * from `trim-strength-477307-h0.learnbiglakeiceberg1.iceberg_demo.person` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR);

#Query in bigquery with data masking works
select concat('****', safe.substr(name, -5)) from `trim-strength-477307-h0.learnbiglakeiceberg1.iceberg_demo.person`;

#Insert sql from BQ not possible
INSERT INTO `trim-strength-477307-h0.learnbiglakeiceberg1.iceberg_demo.person` VALUES (7, "Bobby", 20, F);

Note: this dataset/namespace is not directly visible under BQ table explorer in the UI
      Also in bigquert only select read is possible but no insert operation possible

The dataset itself is not visible and insert sql fails also insert is not at all possible.

#Data masking (policy tag based table level) - NOT possible
There is no option to attach policy tag to the column in the schema of the spark managed iceberg rest catalog table,
more over the rest catalog iceberg table is not visible unde big query dataset.
But at query time we can do data masking using plain sql select.
Ex: select concat('****', safe.substr(name, -5)) from `trim-strength-477307-h0.learnbiglakeiceberg1.iceberg_demo.person`;

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

#Query in bigquery with data masking
select concat('****', safe.substr(name, -5)) from `trim-strength-477307-h0.iceberg_custom_catalog_namespace.person`;

#Time travel is possible for spark managed iceberg (custom catalog)
select * from `trim-strength-477307-h0.iceberg_custom_catalog_namespace.person` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR);

#Data masking column level - NOT possible
There is no option to attach policy tag to the column in the schema of the spark managed iceberg custom catalog table.
Otherwise for big query native table we can create first a taxonomy and then under the taxonomy we can add new policy tags
and enforce access control option under policy tag and then modify the policy tag to include data masking
(select sha256 encryption, or null, or mask first few chars, etc) and add principal who can access
(principal should have data masked reader role).
Then attach these tags to the column in the table column schema.

But at query time we can do data masking using plain sql select.

#Row level access security - NOT Possible -----------------
Row level security is NOT available for BQ managed iceberg table
CREATE ROW ACCESS POLICY
  name_filter_policy
ON
  customer.customer_details_iceberg_bq_managed
    GRANT TO ("user:nitipradhan17@gmail.com")
FILTER USING ("name" = "Kosh");

Error msg:-
BigQuery tables for Apache Iceberg have not been enabled for row-level security.
-----------------------------------------------------------


#Authorized view access - NOT Possible-------------------
create a new view with the same filter criteria and save it in a new dataset and allow the required principal (BQ data viewer role) on this dataset only.
There all views/tables under this new dataset will inherit this permission.
But also make sure to go to the original dataset which contains the BQ managed table and select authorized view option (authorized dataset also available)
and select the newly created view and this will give this new view permission to query tables under the dataset.
(Otherwise under normal view the user should have access to the underlying table as well, but not incase of authorized view)
But in this case of spark managed underlying table when the new principal tries to query the table, permission issue and complains
the user does not have permission storage read on the underlying data and metadata files despite using an authorized view.

Exact error:
Access Denied: BigQuery BigQuery: Permission denied while opening file.
debatosh.pradhan@gmail.com does not have storage.objects.get access to the Google Cloud Storage object.
Permission 'storage.objects.get' denied on resource (or it may not exist).
Please make sure
gs://learnbiglakeiceberg2/warehouse/iceberg_custom_catalog_namespace.db/person/metadata/00003-744a1b9e-7ccf-4341-bdd9-ada681d91ada.metadata.json
is accessible via appropriate IAM roles, e.g. Storage Object Viewer or Storage Object Creator.
Ex:
select * from `trim-strength-477307-ho.iceberg_custom_catalog_namespace.person` where gender = "M";
(save this view in new restricted dataset customer_restricted and share this dataset with new principal)
--------------------------------------------------------

#Data encryption column level - Possible
column level encryption is supported using TBLPROPERTIES and KMS key example:-

CREATE TABLE IF NOT EXISTS iceberg_custom_catalog_namespace.person_encrypted (
  id INT,
  name STRING,
  age INT,
) USING iceberg
TBLPROPERTIES (
  'encryption.key-id'='<your-kms-key-id>',
  'encryption.column.age'='aes-gcm'
)

------------------------------------------------------
# Data lineage tracking - Possible
Able to see the data lineage both from BQ and dataplex where the source metadata.json to the spark managed iceberg table
 to the view created ontop of it.
------------------------------------------------------

###############################################################################################
ICEBERG CUSTOM CATALOG (Spark managed iceberg partitioned table)
###############################################################################################
PYSPARK_FILE="gs://learnbiglakeicerg-artifacts/iceberg_custom_catalog_spark_managed1_with_partition.py"
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
select * from `trim-strength-477307-h0.iceberg_custom_catalog_namespace.person_new`;

#Query in bigquery with data masking
select concat('****', safe.substr(name, -5)) from `trim-strength-477307-h0.iceberg_custom_catalog_namespace.person_new`;

#Time travle is possible for spark managed iceberg (custom catalog)
select * from `trim-strength-477307-h0.iceberg_custom_catalog_namespace.person_new` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR);

###############################################################################################
ICEBERG CUSTOM CATALOG (Spark managed iceberg encryption table)
###############################################################################################
PYSPARK_FILE="gs://learnbiglakeicerg-artifacts/iceberg_custom_catalog_spark_managed1_with_encryption.py"
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
    spark.sql.catalog.${CATALOG_NAME}.warehouse=${WAREHOUSE_DIRECTORY}, \
    spark.sql.catalog.${CATALOG_NAME}.encryption.key-management="gcp-kms", \
    spark.sql.catalog.${CATALOG_NAME}.encryption.gcp.project-id=${PROJECT_ID}"

# Query in Bigquery and no insert possible
select * from `trim-strength-477307-h0.iceberg_custom_catalog_namespace.person_encrypted`;



###############################################################################################
ICEBERG CUSTOM CATALOG (Spark managed iceberg partitioned table) with BQ streaming write
###############################################################################################

When using same code running local to use bq streaming write to iceberg table managed by spark (`trim-strength-477307-h0.iceberg_custom_catalog_namespace.person_new`)
we encounter error as expected as big query client library or simply bq can not be used to write to a spark managed iceberg table.

Exact error message:

Cannot add rows to a table of type BIGQUERY_METASTORE

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

#In big query data masking works
select concat('***', safe.substr(name, -5)) from `trim-strength-477307-h0.customer.customer_details_iceberg_bq_managed`;

# Time travel is possible in bq managed iceberg (custom catalog) table
select * from `trim-strength-477307-h0.customer.customer_details_iceberg_bq_managed` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR);


#Data encryption column level - not directly possible but possible
1) Application-level encryption:
   Encrypt sensitive columns before loading into BigQuery (ex: saferoom encrypts pan column udf first)
   using cloud KMS or client side libraries. Store ciphertext in BigQuery, decrypt only when needed.
2) Data masking/policy tags:
  Use BigQuery data policies + policy tags for column level masking and access control.
  This is governance, not encryption, but often meets compliance needs.
3) Authorized views:
  Restricts access to sensitive columns by exposing only masked or hashed values.

#Authorized view - POSSIBLE ---------------------
create a new view with the same filter criteria and save it in a new dataset and allow the required principal (BQ data viewer role) on this dataset only.
There all views/tables under this new dataset will inherit this permission.
But also make sure to go to the original dataset which contains the BQ managed table and select authorized view option (authorized dataset also available)
and select the newly created view and this will give this new view permission to query tables under the dataset.
(Otherwise under normal view the user should have access to the underlying table as well, but not incase of authorized view)
Ex:
select * from `trim-strength-477307-ho.customer.customer_details_iceberg_bq_managed` where name = "Kosh";
(save this view in new restricted dataset customer_restricted and share this dataset with new principal)
--------------------------------------------------

------------------------------------------------------
# Data lineage tracking - Possible
Able to see the data lineage both from BQ and dataplex where the source metadata.json to the BQ managed iceberg table
 to the view created ontop of it.
------------------------------------------------------
###############################################################################################
ICEBERG CUSTOM CATALOG (BigQuery managed iceberg table) with BQ connector
###############################################################################################
gcloud projects add-iam-policy-binding trim-strength-477307-h0 --member="user:Nitipradhan17@gmail.com" --role="roles/bigquery.connectionUser"
gcloud projects add-iam-policy-binding trim-strength-477307-h0 --member="user:Nitipradhan17@gmail.com" --role="roles/bigquery.connectionAdmin"
gcloud projects add-iam-policy-binding trim-strength-477307-h0 --member="user:Nitipradhan17@gmail.com" --role="roles/bigquery.admin"

gcloud projects add-iam-policy-binding trim-strength-477307-h0 --member="serviceAccount:90486491937-compute@developer.gserviceaccount.com" --role="roles/bigquery.connectionAdmin"

# When doing read only operation on big query managed iceberg table no additional roles were required but when doing write operation using bq connector in spark
# got error : user does not have bigquery.connections.delegate permission for connection <bq conn obj>
# Then gave the bq connection user role to the user or service account

PYSPARK_FILE="gs://learnbiglakeicerg-artifacts/iceberg_custom_catalog_bq_managed_with_bq_connector.py"
PROJECT_ID="trim-strength-477307-h0"
REGION="us-central1"
LOCATION="us-central1"
RUNTIME_VERSION="2.2"
STAGE_BUCKET_PATH="gs://learnbiglakeiceberg3/temp"

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
    spark.dynamicAllocation.enabled=false"

# select and insert both possible on BQ managed iceberg table using BQ connector
Select * from `trim-strength-477307-h0.customer.customer_details_iceberg_bq_managed2`;

###############################################################################################
ICEBERG CUSTOM CATALOG (BigQuery managed iceberg table) with streaming write (using BQ streaming api)
###############################################################################################

run the iceberg_custom_catalog_bq_managed1_with_streaming.py locally and the data gets written out successfully with big query streaming api.
also able to query the table in Big Query and see the newly added records.

select * from `trim-strength-477307-h0.customer.customer_details_iceberg_bq_managed`;

but when checking the table details, the records still show old records (the records which correspond to the data stored in gcs gs://learnbiglakeiceberg3/
also the timestamp of the parquet files present in the gs://learnbiglakeiceberg3/ is old timestamp
but the select * shows all newly inserted records with streaming api.


EDIT:----------------

Again after 20min now the table details properly show exact records count and also the gcs location now shows
(after 20min verified with timestamp) the new parquet files which were written using streaming write apis but the metastore had to be manually exported)


###########################################################################################
ICEBERG CUSTOM CATALOG (Bigquery managed iceberg table) with BQ streaming
##########################################################################################
Run the code locally which uses: insert_rows_json method in the bq client library for streaming records
iceberg_custom_catalog_bq_managed1_with_streaming.py

Note: Although the streaming writes worked fine and select operation aso shows the newly inserted records
but the table details section in BQ studio still shows old less number of records and also the gcs uridoes not yet show the new parquet files with the latest insert timestamp.
May be like earlier after a while the table details and gcs with new file will get updated.

Edit: yes after 30min the table details and file timestamp got updated.
      Also the metadata had to be exported manually to be current.
      
###############################################################################################
ICEBERG CUSTOM CATALOG (Spark managed iceberg partitioned table) with spark streaming
###############################################################################################
PYSPARK_FILE="gs://learnbiglakeicerg-artifacts/iceberg_custom_catalog_spark_managed1_with_streaming.py"
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


Does now work and exact error message:-
Caused by: com.google.cloud.spark.bigquery.repackaged.io.grpc.StatusRuntimeException:
INVALID_ARGUMENT: Can not create stream metadata for table 90486491937:iceberg_custom_catalog_namespace.person with type BIGQUERY_METASTORE.
Allowed type: TABLE, BIGLAKE with streaming. Entity: projects/trim-strength-477307-h0/datasets/iceberg_custom_catalog_namespace/tables/person

###############################################################################################
Normal BQ table streaming works fine as expected
###############################################################################################
normal_bq_native_table_bq_streaming.py (run locally)


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

##########################################################################################
ICEBERG REST CATALOG USING DEFAULT REST CATALOG BUCKET
##########################################################################################
PYSPARK_FILE="gs://learnbiglakeicerg-artifacts/iceberg_rest_catalog_spark_managed_with_default_cat_dir.py"
PROJECT_ID="trim-strength-477307-h0"
REGION="us-central1"
LOCATION="us-central1"
RUNTIME_VERSION="2.2"
CATALOG_NAME="learnbiglakeiceberg5"
WAREHOUSE_DIRECTORY="gs://learnbiglakeiceberg5"
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

#########################################################################################




###############################################################################################
ICEBERG REST CATALOG (spark managed) - when the rest catalog is not pre created in gcp console (WORKS)
###############################################################################################
# Replace these variables with your values
PYSPARK_FILE="gs://learnbiglakeicerg-artifacts/iceberg_rest_catalog_spark_managed_restcat_not_precreated.py"
PROJECT_ID="trim-strength-477307-h0"
REGION="us-central1"
RUNTIME_VERSION="2.2"
CATALOG_NAME="learnbiglakeiceberg10"
STAGE_BUCKET_PATH="gs://dataproc_job_staging_bucket"
WAREHOUSE_PATH="gs://learnbiglakeiceberg10"


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
Note: The dataset "iceberg_demo" is not visible in big query studio
select * from `trim-strength-477307-h0.learnbiglakeiceberg10.iceberg_demo.person`;

#Time travel is possible for spark managed iceberg (rest catalog)
select * from `trim-strength-477307-h0.learnbiglakeiceberg10.iceberg_demo.person` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR);

###############################################################################################
ICEBERG REST CATALOG (spark managed) - when the rest catalog is pre created with credential vending mode in gcp console (WORKS)
###############################################################################################
# Replace these variables with your values
PYSPARK_FILE="gs://learnbiglakeicerg-artifacts/iceberg_rest_catalog_spark_managed_restcat_precreated_credvenmode.py"
PROJECT_ID="trim-strength-477307-h0"
REGION="us-central1"
RUNTIME_VERSION="2.3"
CATALOG_NAME="learnbiglakeiceberg11"
STAGE_BUCKET_PATH="gs://dataproc_job_staging_bucket"
WAREHOUSE_PATH="gs://learnbiglakeiceberg11"


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
spark.sql.catalog.${CATALOG_NAME}.header.X-Iceberg-Access-Delegation=vended-credentials,\
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,\
spark.sql.catalog.${CATALOG_NAME}.rest-metrics-reporting-enabled=false"


select * from `trim-strength-477307-h0.learnbiglakeiceberg11.iceberg_demo.person`;

#without granting the delegate service account the permission on the bucket we get permission error:-
Forbidden: Permission denied to write to GCS file: gs://learnbiglakeiceberg11/iceberg_demo/person/metadata/00000-699043ac-0000-21ac-861c-14223bc60f42.metadata.json:
blirc-90486491937-ch9p@gcp-sa-biglakerestcatalog.iam.gserviceaccount.com does not have storage.objects.create access to the Google Cloud Storage object.
Permission 'storage.objects.create' denied on resource (or it may not exist).

#Now gave the delegate service account: blirc-90486491937-ch9p@gcp-sa-biglakerestcatalog.iam.gserviceaccount.com
# roles/storage.objectUser on the bucket: gs://learnbiglakeiceberg11
Note: After using RUNTIME_VERSION="2.3", it started working and using cred vended mode able to write data files to the bucket
otherwise I was only able to create table and create metadata in gcs bucket and when I would want to write data getting error like null endpoint for credentials.

Note: only gave roles/storage.objectUser to delegate service account: blirc-90486491937-ch9p@gcp-sa-biglakerestcatalog.iam.gserviceaccount.com
      and biglake editor role to the principal nitipradhan17 for both writing and reading using rest cat cred vending mode.
      no extra role given to dataproc service agent etc




