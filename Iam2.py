import boto3
import psycopg2
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


class IAMPostgresOperator(SQLExecuteQueryOperator):
    def __init__(self, aws_region, db_user, db_name, host, port=5432, **kwargs):
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
        # Dynamically create a connection using IAM
        from airflow.providers.common.sql.hooks.sql import DbApiHook

        class SimplePostgresHook(DbApiHook):
            def get_conn(inner_self):
                token = self.get_iam_token()
                conn_str = (
                    f"postgresql://{self.db_user}:{token}@{self.host}:{self.port}/{self.db_name}"
                )
                return psycopg2.connect(conn_str, sslmode='require')

        return SimplePostgresHook()

