import json

def avro_to_bigquery_schema(avro_schema):
    """
    Converts an Avro schema to a BigQuery schema.
    
    Args:
        avro_schema (dict): The Avro schema as a dictionary.
    
    Returns:
        list: A list of dictionaries representing the BigQuery schema.
    """
    type_mapping = {
        "null": "STRING",  # BigQuery does not have a NULL type, so we map it to STRING
        "boolean": "BOOLEAN",
        "int": "INTEGER",
        "long": "INTEGER",
        "float": "FLOAT",
        "double": "FLOAT",
        "bytes": "BYTES",
        "string": "STRING",
        "record": "RECORD",
        "enum": "STRING",
        "array": "ARRAY",
        "map": "STRING",  # BigQuery does not have a MAP type, so we map it to STRING
        "fixed": "BYTES",
    }

    bigquery_schema = []

    for field in avro_schema.get("fields", []):
        field_name = field["name"]
        field_type = field["type"]

        # Handle union types (e.g., ["null", "string"])
        if isinstance(field_type, list):
            # Filter out "null" and use the first non-null type
            non_null_types = [t for t in field_type if t != "null"]
            if non_null_types:
                field_type = non_null_types[0]
            else:
                field_type = "null"

        # Handle nested records
        if isinstance(field_type, dict) and field_type.get("type") == "record":
            nested_schema = avro_to_bigquery_schema(field_type)
            bigquery_field = {
                "name": field_name,
                "type": "RECORD",
                "mode": "REPEATED" if field_type.get("type") == "array" else "NULLABLE",
                "fields": nested_schema,
            }
        # Handle arrays
        elif isinstance(field_type, dict) and field_type.get("type") == "array":
            array_type = field_type["items"]
            if isinstance(array_type, dict) and array_type.get("type") == "record":
                nested_schema = avro_to_bigquery_schema(array_type)
                bigquery_field = {
                    "name": field_name,
                    "type": "RECORD",
                    "mode": "REPEATED",
                    "fields": nested_schema,
                }
            else:
                bigquery_field = {
                    "name": field_name,
                    "type": type_mapping.get(array_type, "STRING"),
                    "mode": "REPEATED",
                }
        else:
            bigquery_field = {
                "name": field_name,
                "type": type_mapping.get(field_type, "STRING"),
                "mode": "NULLABLE",
            }

        bigquery_schema.append(bigquery_field)

    return bigquery_schema

def read_avro_schema_from_json(file_path):
    """
    Reads an Avro schema from a JSON file.
    
    Args:
        file_path (str): Path to the JSON file containing the Avro schema.
    
    Returns:
        dict: The Avro schema as a dictionary.
    """
    with open(file_path, "r") as f:
        return json.load(f)

# Example usage
if __name__ == "__main__":
    # Path to the Avro schema JSON file
    avro_schema_file = "path/to/avro_schema.json"

    # Read the Avro schema from the JSON file
    avro_schema = read_avro_schema_from_json(avro_schema_file)

    # Convert Avro schema to BigQuery schema
    bigquery_schema = avro_to_bigquery_schema(avro_schema)

    # Print the BigQuery schema
    print(json.dumps(bigquery_schema, indent=2))
