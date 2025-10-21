from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowPythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='simple_dataflow_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['dataflow', 'composer'],
) as dag:
    run_dataflow_job = DataflowPythonOperator(
        task_id='run_wordcount_job',
        py_file='gs://your-gcs-bucket/dataflow_scripts/wordcount.py', # Path to your Dataflow script in GCS
        options={
            'input': 'gs://your-gcs-bucket/input/sample.txt', # Input file in GCS
            'output': 'gs://your-gcs-bucket/output/wordcounts', # Output prefix in GCS
            'project': 'your-gcp-project-id',
            'region': 'your-gcp-region',
            'temp_location': 'gs://your-gcs-bucket/temp',
            'staging_location': 'gs://your-gcs-bucket/staging',
        },
        dataflow_default_options={
            'project': 'your-gcp-project-id',
            'region': 'your-gcp-region',
        },
    )
