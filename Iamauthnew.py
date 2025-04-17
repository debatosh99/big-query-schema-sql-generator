import boto3
import psycopg2
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.base import BaseHook


class IAMPostgresOperator(SQLExecuteQueryOperator):
    def __init__(self, aws_region, db_user, db_name, host, port, **kwargs):
        self.aws_region = aws_region
        self.db_user = db_user
        self.db_name = db_name
        self.host = host
        self.port = port
        super().__init__(**kwargs)

    def get_iam_token(self):
        rds_client = boto3.client("rds", region_name=self.aws_region)
        token = rds_client.generate_db_auth_token(
            DBHostname=self.host,
            Port=self.port,
            DBUsername=self.db_user
        )
        return token

    def get_db_hook(self):
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        # Use IAM token instead of password
        iam_token = self.get_iam_token()

        conn = PostgresHook(
            postgres_conn_id=None,
            host=self.host,
            login=self.db_user,
            password=iam_token,
            schema=self.db_name,
            port=self.port
        )
        return conn


_------------
iam_pg_op = IAMPostgresOperator(
    task_id='iam_postgres_query',
    aws_region='us-west-2',
    db_user='your_db_user',
    db_name='your_db_name',
    host='your-db-host.rds.amazonaws.com',
    port=5432,
    sql='SELECT NOW();'
)
