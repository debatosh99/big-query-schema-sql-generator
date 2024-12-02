import json
import copy

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



final_sql_list = []
bq_schema_list = load_json("bq-schema5.json")
parse_bq_schema(bq_schema_list)
print(final_sql_list)
sql_query = generate_sql_query(final_sql_list)
print(sql_query)






