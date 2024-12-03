import json

def get_type_from_schema(schema, path):
    """
    Recursively searches a BigQuery schema to find the type of a field specified by a dot-separated path.

    Args:
        schema: The BigQuery schema, as a list of field dictionaries.
        path: The dot-separated path to the field.

    Returns:
        The type of the field, or None if the path is not found.
    """

    for field in schema:
        if field['name'] == path.split('.')[0]:
            if len(path.split('.')) == 1:
                return field['type']
            else:
                return get_type_from_schema(field['fields'], '.'.join(path.split('.')[1:]))

    return None

# Example usage:
schema = [
    # Your schema from the prompt
]

def load_json(file_path):
    """Loads a json file into a python object(list/dict)"""
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data

bq_schema_list = load_json("bq-schema5.json")
# path = "personalInfo.bankDetails.account.ifsc"
path = "personalInfo.first_name"
field_type = get_type_from_schema(bq_schema_list, path)
print(field_type)  # Output: NUMERIC