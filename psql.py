from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import subprocess
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'psql_subprocess_dag',
    default_args=default_args,
    description='A DAG to execute PostgreSQL queries using psql and subprocess',
    schedule_interval=None,  # Manual trigger
    catchup=False,
)

def execute_psql_query():
    # Retrieve PostgreSQL credentials from environment variables
    pg_user = os.getenv('PGUSER')
    pg_password = os.getenv('PGPASSWORD')
    pg_host = os.getenv('PGHOST')
    pg_port = os.getenv('PGPORT')
    pg_database = os.getenv('PGDATABASE')

    if not all([pg_user, pg_password, pg_host, pg_port, pg_database]):
        raise ValueError("One or more PostgreSQL environment variables are missing!")

    # Define the query to execute
    query = "SELECT * FROM your_table LIMIT 10;"

    # Construct the psql command
    psql_command = [
        'psql',
        '-h', pg_host,
        '-p', pg_port,
        '-U', pg_user,
        '-d', pg_database,
        '-c', query
    ]

    # Set the PGPASSWORD environment variable for the subprocess
    env = os.environ.copy()
    env['PGPASSWORD'] = pg_password

    # Execute the psql command
    try:
        result = subprocess.run(
            psql_command,
            env=env,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        print("Query Output:")
        print(result.stdout)
    except subprocess.Called
