import json

def avro_schema_to_bigquery_schema(avro_schema):
    """Converts an Avro schema to a BigQuery schema."""
    bigquery_schema = []
    for field in avro_schema["fields"]:
        bigquery_field = {"name": field["name"]}
        if isinstance(field["type"], str):
            bigquery_field["type"] = field["type"].upper()
        elif isinstance(field["type"], dict) and field["type"]["type"] == "array":
            bigquery_field["type"] = field["type"]["items"].upper()
            bigquery_field["mode"] = "REPEATED"
        elif isinstance(field["type"], dict) and field["type"]["type"] == "record":
            bigquery_field["type"] = "RECORD"
            bigquery_field["fields"] = avro_schema_to_bigquery_schema({"fields": field["type"]["fields"]})
        else:
            print(f"unknown type: {field['type']}")
        bigquery_schema.append(bigquery_field)
    return bigquery_schema

def convert_avro_to_bigquery(avro_schema_file, bigquery_schema_file):
    with open(avro_schema_file, 'r') as f:
        avro_schema = json.load(f)

    bigquery_schema = avro_schema_to_bigquery_schema(avro_schema)

    with open(bigquery_schema_file, 'w') as f:
        json.dump(bigquery_schema, f, indent=2)

#example usage
convert_avro_to_bigquery("avro_schema.json", "bigquery_schema.json")
