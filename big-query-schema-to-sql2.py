import json
import copy
import os
from google.cloud import bigquery


def load_json(file_path):
    """Loads a json file into a python object(list/dict)"""
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data

def parse_each_attribute(attr_dict: dict):
    if isinstance(attr_dict, dict) and (attr_dict.get("fields") is None):
        each_column = attr_dict.get("name") + " as " + str(attr_dict.get("name")).split(".")[-1]
        final_sql_list.append(each_column)
        #print(each_column)


def appender(fields : list[dict], append_name: str):
    for attribute in fields:
        attribute["name"] = append_name+"."+attribute.get("name")
    return fields

def parse_bq_schema(bq_schema_list: list[dict]):
    for attribute in bq_schema_list:
        if isinstance(attribute, dict) and (attribute.get("fields") is None):
            parse_each_attribute(attribute)
        elif isinstance(attribute, dict) and (attribute.get("fields") is not None):
            appended_fields_list = appender(attribute.get("fields"), attribute.get("name"))
            parse_bq_schema(appended_fields_list)


def generate_sql_query(columns):
    """Generates a SQL SELECT statement from a list of column specifications.
    Args:
        columns: A list of strings in the format "source_column as alias".
    Returns:
        The generated SQL SELECT statement.
    """
    select_clause = "SELECT "
    from_clause = "FROM `playground-s-11-1b5c1ab9.test_dataset.student_records`"

    previousNoOfChild = 0
    currentNoOfChild = 0
    noOfStructOpen = 0
    openStructsNames = []
    for column in columns:
        source_column, alias = column.split(" as ")
        if "." in source_column:
            # Nested structure, create a STRUCT and keep track of how many child nodes
            parts = source_column.split(".")
            currentNoOfChild = len(parts) - 1
            struct_alias = parts[-2]
            if currentNoOfChild > previousNoOfChild:
                select_clause += f"STRUCT({source_column} AS {alias},"
                openStructsNames.append(struct_alias)
                noOfStructOpen+=1
            elif currentNoOfChild == previousNoOfChild:
                select_clause += f"{source_column} AS {alias},"
            elif currentNoOfChild < previousNoOfChild:
                select_clause = select_clause.rstrip(",")
                select_clause += f") as {openStructsNames.pop(-1)},"
                noOfStructOpen = noOfStructOpen -1
                select_clause += f"{source_column} AS {alias},"
            else:
                select_clause += f"{source_column} AS {alias},"
            previousNoOfChild = currentNoOfChild
        elif noOfStructOpen != 0 and ("." not in source_column):
            for i in range(0,len(openStructsNames)):
                select_clause = select_clause.rstrip(",")
                select_clause += f") as {openStructsNames.pop(-1)}"
                noOfStructOpen = noOfStructOpen - 1
            select_clause += f", {source_column} AS {alias},"
        # elif noOfStructOpen == 0 and ("." not in source_column):
        elif noOfStructOpen == 0 and ("." not in source_column):
            # Simple column
            select_clause += f"{source_column} AS {alias},"

    # Remove the trailing comma and space
    select_clause = select_clause[:-1]

    sql_query = f"{select_clause} {from_clause}"
    return sql_query



def generate_encrypt_sql(sqlstatement : str, encrypt_cols : list, full_func_name : str) -> str:
    sql_statement_str = sqlstatement
    for col in encrypt_cols:
        encrypt_func = f"{full_func_name}(CAST({col} AS STRING))"
        sql_statement_str = sql_statement_str.replace(col, encrypt_func)
    return sql_statement_str


def get_table_schema(project_id, dataset_name, table_name):
    table_id = f"{project_id}.{dataset_name}.{table_name}"
    client = bigquery.Client()
    table = client.get_table(table_id)
    schema = table.schema
    return schema

def _convert_field(field):
    field_dict = {
        "name": field.name,
        "type": field.field_type,
        "mode": field.mode
    }
    if field.fields:
        field_dict["fields"] = [_convert_field(f) for f in field.fields]
    return field_dict

def bq_schema_to_json(project_id, dataset_name, table_name):
    bq_schema = get_table_schema(project_id, dataset_name, table_name)
    schema_json = [_convert_field(field) for field in bq_schema]
    return schema_json


# Specify the project ID, dataset name, and table name
project_id = "playground-s-11-42147a98"
dataset_name = "test_dataset"
table_name = "student_records"
func_name = "strcon"
full_func_name = f"{dataset_name}.{func_name}"
encrypt_cols = ["personalInfo.bankDetails.account.accountNo", "department"]
# Construct a fully qualified table name
table_id = f"{project_id}.{dataset_name}.{table_name}"
final_sql_list = []
# bq_schema_list = bq_schema_to_json(project_id, dataset_name, table_name)
# print(json.dumps(bq_schema_list, indent=4))
bq_schema_list = load_json("bq-schema5.json")
parse_bq_schema(bq_schema_list)
print(final_sql_list)
sql_query = generate_sql_query(final_sql_list)
print(sql_query)
encrypted_sql_query = generate_encrypt_sql(sql_query, encrypt_cols, full_func_name)
print(encrypted_sql_query)






