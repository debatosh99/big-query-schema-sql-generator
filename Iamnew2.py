from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Sequence, Union

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.google.cloud.hooks.cloud_sql import CloudSQLHook

class CloudSqlExecuteQueryOperatorWithIAM(SQLExecuteQueryOperator):
    """
    Executes a SQL query in a Cloud SQL database using IAM authentication.

    This operator inherits from SQLExecuteQueryOperator and extends it to support
    Google Cloud SQL IAM authentication.  It retrieves an authentication token
    and adds it to the connection parameters.

    :param cloud_sql_conn_id: The Airflow connection ID for the Cloud SQL instance.
    :param sql: The SQL query to execute.
    :param autocommit: Whether to autocommit the query.
    :param parameters: (Optional) Parameters to pass to the SQL query.
    :param database: (Optional) The Cloud SQL database to use.  If not provided,
        it uses the database defined in the connection.
    :param gcp_conn_id: (Optional) The GCP connection ID to use for authentication.
        If None, the default GCP connection is used.
    :param use_legacy_sql: (Optional)  Whether to use legacy SQL.
    """

    template_fields: Sequence[str] = ("sql", "parameters", "database")
    template_ext: Sequence[str] = (".sql",)
    ui_color = "#f4a460"  #  A nice orange color for the operator in the UI

    def __init(
        self,
        *,
        cloud_sql_conn_id: str,
        sql: str,
        autocommit: bool = False,
        parameters: Optional[dict | list] = None,
        database: Optional[str] = None,
        gcp_conn_id: str = "google_cloud_default",
        use_legacy_sql: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            sql=sql,
            autocommit=autocommit,
            parameters=parameters,
            **kwargs,
        )
        self.cloud_sql_conn_id = cloud_sql_conn_id
        self.database = database
        self.gcp_conn_id = gcp_conn_id
        self.use_legacy_sql = use_legacy_sql

    def execute(self, context: Dict[str, Any]) -> Any:
        """
        Executes the SQL query with IAM authentication.
        """
        hook = CloudSQLHook(cloud_sql_conn_id=self.cloud_sql_conn_id, gcp_conn_id=self.gcp_conn_id)

        # Get the connection object.
        conn = hook.get_connection()

        if conn.extra_dejson.get("use_iam_auth") is True:
            logging.info("Using IAM authentication for Cloud SQL.")
            # Get an IAM authentication token.
            iam_token = hook.get_iam_token()
            # Update the connection parameters with the IAM token.  The key
            #  is driver-specific.  For example, for psycopg2 (PostgreSQL):
            if conn.conn_type == "postgres":
                conn.extra_dejson["password"] = iam_token
            elif conn.conn_type == "mysql":
                conn.extra_dejson["access_token"] = iam_token
            else:
                raise Exception(f"IAM authentication is not supported for conn_type: {conn.conn_type}")
        elif conn.extra_dejson.get("use_iam_auth") is False:
            logging.info("IAM authentication is explicitly disabled in the connection.")
        else:
            logging.info("Not using IAM authentication for Cloud SQL.  Check your connection settings.")

        # Get a database connection.
        engine = hook.get_conn()

        if self.database:
            engine.database = self.database

        cursor = engine.cursor()

        if self.use_legacy_sql:
            cursor.execute(self.sql, self.parameters)
        else:
            cursor.execute(self.sql, self.parameters)

        if self.autocommit:
            engine.commit()

        if self.do_xcom_push:
            return cursor.fetchall()
        return None
