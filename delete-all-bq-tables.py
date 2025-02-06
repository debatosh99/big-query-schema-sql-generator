from google.cloud import bigquery

def delete_tables_with_postfix(project_id, dataset_id, postfix):
    # Initialize the BigQuery client
    client = bigquery.Client(project=project_id)

    # Get the dataset reference
    dataset_ref = client.dataset(dataset_id)

    # List all tables in the dataset
    tables = list(client.list_tables(dataset_ref))

    # Iterate through the tables and delete those with the matching postfix
    for table in tables:
        table_name = table.table_id
        if table_name.endswith(postfix):
            table_ref = dataset_ref.table(table_name)
            print(f"Deleting table: {table_ref}")
            client.delete_table(table_ref, not_found_ok=True)  # Delete the table

    print("Deletion process completed.")

# Example usage
project_id = "your-project-id"  # Replace with your GCP project ID
dataset_id = "your-dataset-id"  # Replace with your BigQuery dataset ID
postfix = "pqr"  # Replace with the postfix you want to match

delete_tables_with_postfix(project_id, dataset_id, postfix)
