from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.base import BaseHook
import boto3
import pymysql  # or use psycopg2 for PostgreSQL
import os
import datetime

class IAMSQLExecuteQueryOperator(SQLExecuteQueryOperator):

    def _generate_iam_token(self, host, port, user, region):
        rds_client = boto3.client('rds', region_name=region)
        token = rds_client.generate_db_auth_token(DBHostname=host, Port=port, DBUsername=user)
        return token

    def get_db_hook(self):
        conn = BaseHook.get_connection(self.conn_id)

        host = conn.host
        port = conn.port or 3306
        user = conn.login
        db_name = conn.schema
        region = os.getenv('AWS_REGION', 'us-west-2')

        token = self._generate_iam_token(host, port, user, region)

        # Override the password with the IAM token
        conn.password = token

        # Choose correct hook class depending on DB type
        from airflow.providers.mysql.hooks.mysql import MySqlHook
        # from airflow.providers.postgres.hooks.postgres import PostgresHook

        return MySqlHook(
            mysql_conn_id=None,
            schema=db_name,
            host=host,
            login=user,
            password=token,
            port=port
        )
