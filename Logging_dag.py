from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.logging import LoggingListSinksOperator
from airflow.providers.google.cloud.operators.logging import LoggingCreateSinkOperator
from airflow.providers.google.cloud.operators.logging import LoggingDeleteSinkOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import json

DAG_ID = "gcs_activity_to_cloudsql"
PROJECT_ID = "your-gcp-project-id"
DATASET_ID = "your_bigquery_dataset"
TABLE_ID = "gcs_activity_logs"
BUCKET_NAME = "your-gcs-bucket-for-logging"
SINK_NAME = "gcs-activity-sink"
POSTGRES_CONN_ID = "your_postgres_conn_id"  # Create this in Airflow Connections

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    catchup=False,
) as dag:
    # 1. Create a logging sink to export GCS activity logs to BigQuery.
    create_sink = LoggingCreateSinkOperator(
        task_id="create_logging_sink",
        sink_name=SINK_NAME,
        filter_="",  # Filter to capture all logs
        destination=f"bigquery.googleapis.com/projects/{PROJECT_ID}/datasets/{DATASET_ID}",
        unique_writer_identity=True,
        include_children=True,
    )

    # 2. Extract logs from BigQuery and transform them.
    extract_and_transform = BigQueryExecuteQueryOperator(
        task_id="extract_and_transform_logs",
        sql=f"""
        SELECT
            JSON_VALUE(resource.labels.bucket_name) AS bucket_name,
            JSON_VALUE(protoPayload.resourceName) AS blob_name,
            CASE
                WHEN protoPayload.methodName LIKE '%insert%' THEN 'WRITE'
                WHEN protoPayload.methodName LIKE '%delete%' THEN 'DELETE'
                WHEN protoPayload.methodName LIKE '%get%' THEN 'READ'
                ELSE 'UNKNOWN'
            END AS operation,
            TIMESTAMP_MICROS(protoPayload.authenticationInfo.principalSubject.integerId) as timestamp,
            protoPayload.authenticationInfo.principalEmail as user_email
        FROM
            `{PROJECT_ID}.{DATASET_ID}.{SINK_NAME}_logs`
        WHERE
            resource.type = 'gcs_bucket'
            AND protoPayload.methodName IS NOT NULL
            AND JSON_VALUE(resource.labels.bucket_name) = '{BUCKET_NAME}'
            AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
        """,
        destination_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}_temp",
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
    )

    # 3. Create a Postgres table if it doesn't exist.
    create_postgres_table = PostgresOperator(
        task_id="create_postgres_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS gcs_activity (
            bucket_name VARCHAR(255),
            blob_name VARCHAR(2048),
            operation VARCHAR(10),
            timestamp TIMESTAMP,
            user_email VARCHAR(255)
        );
        """,
    )

    # 4. Load data from BigQuery to Postgres.
    bigquery_to_postgres = BigQueryExecuteQueryOperator(
        task_id="load_bigquery_to_postgres",
        sql=f"""
        INSERT INTO gcs_activity (bucket_name, blob_name, operation, timestamp, user_email)
        SELECT bucket_name, blob_name, operation, timestamp, user_email
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}_temp`;
        """,
        destination_dataset_table=None,
        write_disposition="WRITE_APPEND",
        use_legacy_sql=False,
        postgres_conn_id=POSTGRES_CONN_ID,
    )

    # 5. Delete the logging sink (optional).
    delete_sink = LoggingDeleteSinkOperator(
        task_id="delete_logging_sink",
        sink_name=SINK_NAME,
        trigger_rule="all_done", # Ensure deletion happens even if other tasks fail.
    )

    # Define task dependencies.
    create_sink >> extract_and_transform >> create_postgres_table >> bigquery_to_postgres >> delete_sink
