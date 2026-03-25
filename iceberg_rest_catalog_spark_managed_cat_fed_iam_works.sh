gcloud beta biglake iceberg catalogs get-iam-policy learnbiglakeiceberg18 \
    --project=trim-strength-477307-h0 \
    --format=json > learnbiglakeiceberg18_policy.json

{
  "etag": "ACAB",
  "bindings": [
    {
      "members": [
         "serviceAccount:deb2-592@trim-strength-477307-h0.iam.gserviceaccount.com",
        "serviceAccount:deb1-591@trim-strength-477307-h0.iam.gserviceaccount.com"
      ],
      "role": "roles/biglake.editor"
    }
  ]
}

gcloud beta biglake iceberg catalogs set-iam-policy learnbiglakeiceberg18 learnbiglakeiceberg18_policy.json --project=trim-strength-477307-h0


PYSPARK_FILE="gs://learnbiglakeicerg-artifacts/iceberg_rest_catalog_spark_managed_cat_fed_iam_works.py"
PROJECT_ID="trim-strength-477307-h0"
REGION="us-central1"
RUNTIME_VERSION="2.3"
CATALOG_NAME="learnbiglakeiceberg18"
WAREHOUSE_PATH="bq://projects/trim-strength-477307-h0/locations/us-central1"
STAGE_BUCKET_PATH="gs://dataproc_job_staging_bucket"
SERVICE_ACCOUNT="deb1-591@trim-strength-477307-h0.iam.gserviceaccount.com"



# Submit the Spark job
gcloud dataproc batches submit pyspark ${PYSPARK_FILE} \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --service-account=${SERVICE_ACCOUNT} \
    --version=${RUNTIME_VERSION} \
    --deps-bucket=${STAGE_BUCKET_PATH} \
    --properties="\
    spark.driver.cores=1,\
    spark.driver.memory=1g,\
    spark.executor.cores=1,\
    spark.executor.memory=1g,\
    spark.executor.instances=1,\
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

# Query in Bigquery
select * from `trim-strength-477307-h0.iceberg_demo_now.person_now`;  (WORKS)

select * from `trim-strength-477307-h0.learnbiglakeiceberg18.iceberg_demo_now.person_now`; (P.C.N.T) - (DOES NOT WORK since already catalog federation done)


gcloud alpha biglake iceberg namespaces list \
    --catalog=learnbiglakeiceberg18 \
    --project=trim-strength-477307-h0

NAME: projects/trim-strength-477307-h0/catalogs/learnbiglakeiceberg18/namespaces/iceberg_demo2
NAMESPACE-ID: iceberg_demo2

Note: It shows only that namespace where catalog federation has not yet been successfully done

gcloud alpha biglake iceberg tables list \
    --catalog=learnbiglakeiceberg18 \
    --namespace=iceberg_demo_now \
    --project=trim-strength-477307-h0

ERROR: (gcloud.alpha.biglake.iceberg.tables.list) HTTPError 404: Namespace does not exist: iceberg_demo_now. This command is authenticated as nitipradhan17@gmail.com which is the active account specified by the [core/account] property




