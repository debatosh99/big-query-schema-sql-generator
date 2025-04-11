import logging
import subprocess
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
from contextlib import contextmanager
from typing import List, Dict
import os
import tempfile

logger = logging.getLogger(__name__)

@contextmanager
def temporary_token_file(token: str):
    """Securely handle temporary token file with automatic cleanup"""
    try:
        # Create secure temp file (automatically deleted when closed)
        with tempfile.NamedTemporaryFile(mode='w+', delete=True) as temp_file:
            temp_file.write(token)
            temp_file.flush()
            os.fsync(temp_file.fileno())  # Ensure token is written to disk
            yield temp_file.name
    except Exception as e:
        logger.error("Failed to handle temporary token file", exc_info=False)
        raise AirflowException("Token file handling failed")

def generate_iam_token(service_account: str) -> str:
    """Securely generate IAM authentication token"""
    try:
        cmd = [
            "gcloud",
            "sql",
            "generate-login-token",
            f"--impersonate-service-account={service_account}",
            "--quiet"
        ]
        
        # Execute with secure environment
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            env={**os.environ, "PYTHONUNBUFFERED": "1"}  # Clean environment
        )
        
        token = result.stdout.strip()
        if not token:
            raise ValueError("Empty token received from gcloud")
            
        return token
    except subprocess.CalledProcessError as e:
        logger.error(f"gcloud command failed with return code {e.returncode}", exc_info=False)
        raise AirflowException(f"IAM token generation failed: {e.stderr}")
    except Exception as e:
        logger.error("Unexpected error during token generation", exc_info=False)
        raise AirflowException("IAM token generation failed")

@contextmanager
def secure_cloudsql_connection(conn_id: str):
    """Secure context manager for CloudSQL IAM connection"""
    try:
        conn = BaseHook.get_connection(conn_id)
        service_account = conn.login
        
        if not service_account.endswith('.gserviceaccount.com'):
            raise ValueError("Invalid service account format")
            
        # Generate token securely
        token = generate_iam_token(service_account)
        
        # Create connection string without logging sensitive info
        db_user = service_account.split('.gserviceaccount.com')[0]
        conn_string = (
            f"postgresql+psycopg2://{db_user}:{token}"
            f"@{conn.host}:{conn.port}/{conn.schema}"
            f"?sslmode=require"
        )
        
        # Create engine with secure settings
        engine = None
        try:
            engine = create_engine(
                conn_string,
                connect_args={
                    'sslmode': 'require',
                    'sslrootcert': '/etc/ssl/certs/ca-certificates.crt'
                },
                pool_pre_ping=True,
                hide_parameters=True,
                echo=False  # Disable engine logging
            )
            yield engine
        finally:
            if engine:
                engine.dispose()
    except Exception as e:
        logger.error("CloudSQL connection setup failed", exc_info=False)
        raise AirflowException("Failed to establish secure database connection")

def secure_cloudsql_iam_write(**kwargs) -> int:
    """
    Securely writes DataFrame to CloudSQL using IAM authentication
    
    Args:
        kwargs: Airflow context with 'params' containing:
            - table_name: Target table name
            - schema_name: Database schema name
            - data: List of dictionaries with data to insert
    
    Returns:
        Number of rows inserted
        
    Raises:
        AirflowException: For any security or operational failure
    """
    try:
        # Validate and extract parameters
        params = kwargs.get('params', {})
        table_name = params.get('table_name', 'saferm_fl_pttn_ctl_log')
        schema_name = params.get('schema_name', 'saferoom')
        data = params.get('data', [{"reg_key": 9004, "status": "init"}])
        
        # Create DataFrame with validation
        if not isinstance(data, list) or not all(isinstance(x, dict) for x in data):
            raise ValueError("Input data must be a list of dictionaries")
            
        df = pd.DataFrame(data)
        required_columns = {"reg_key", "status"}
        if not required_columns.issubset(df.columns):
            raise ValueError(f"DataFrame must contain columns: {required_columns}")
        
        # Perform secure database operation
        with secure_cloudsql_connection("saferoom_conn_test") as engine:
            rows_inserted = df.to_sql(
                name=table_name,
                con=engine,
                schema=schema_name,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            
            logger.info(f"Successfully inserted {rows_inserted} rows into {schema_name}.{table_name}")
            return rows_inserted
            
    except SQLAlchemyError as e:
        logger.error("Database operation failed", exc_info=False)
        raise AirflowException(f"Database write failed: {str(e)}")
    except Exception as e:
        logger.error("Unexpected error in CloudSQL operation", exc_info=False)
        raise AirflowException(f"Operation failed: {str(e)}")
