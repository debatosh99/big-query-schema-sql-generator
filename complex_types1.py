from google.cloud import bigquery

def generate_create_table_sql(project_name: str, dataset_name: str, table_name: str) -> str:
    """
    Generates a SQL query to select all fields from a BigQuery table,
    handling complex data types, in a way that is suitable for creating
    an exact copy of the table using CREATE TABLE AS SELECT.

    Args:
        project_name: The name of the BigQuery project.
        dataset_name: The name of the BigQuery dataset.
        table_name: The name of the BigQuery table.

    Returns:
        A SQL query string.

    Raises:
        google.cloud.exceptions.NotFound: If the table is not found.
    """
    client = bigquery.Client(project=project_name)
    table_id = f"{project_name}.{dataset_name}.{table_name}"
    try:
        table = client.get_table(table_id)  # Raises NotFound if table does not exist.
    except Exception as e:
        print(f"Error getting table: {e}")
        raise

    select_fields = []
    for field in table.schema:
        select_fields.append(f"`{field.name}`")  # Quote field names

    sql = f"""
        SELECT
            {', '.join(select_fields)}
        FROM
            `{project_name}.{dataset_name}.{table_name}`
    """
    return sql

def main():
    """
    Main function to demonstrate usage.  Prompts for user input.
    """
    project_name = input("Enter your BigQuery Project Name: ")
    dataset_name = input("Enter your BigQuery Dataset Name: ")
    table_name = input("Enter your BigQuery Table Name: ")

    try:
        sql = generate_create_table_sql(project_name, dataset_name, table_name)
        print("\nGenerated SQL Query:")
        print(sql)

        #  Optional:  Example of using the SQL to create a new table (requires a new table name)
        new_table_name = input(
            "Enter a name for the new table (or leave blank to skip creation): "
        )
        if new_table_name:
            client = bigquery.Client(project=project_name)
            new_table_id = f"{project_name}.{dataset_name}.{new_table_name}"
            query_job = client.query(sql)  #  Run the query.
            query_job.result()  # Wait for the query to complete.

            create_table_query = f"""
                CREATE TABLE {new_table_id} AS
                {sql}
            """
            create_table_job = client.query(create_table_query)
            create_table_job.result()

            print(f"New table '{new_table_id}' created successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()

