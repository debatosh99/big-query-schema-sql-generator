#######################################################################
# FIRST use: gcloud auth application-default login (doubtful)
# run this file in gcp shell editor
# worked when simply opened cloud shell editor and simply ran this python file without even doing gcloud auth application-default login
#######################################################################

import pyarrow as pa
import google.auth
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField

# Initialize Spark Session
spark = SparkSession.builder.getOrCreate()

# 1. Fetch Credentials safely
# 'google.auth.default' will automatically find your 'gcloud auth' credentials
credentials, project_id = google.auth.default()

# Robust way to ensure token is present without manual refresh crashes
from google.auth.transport.requests import Request
if not credentials.token:
    credentials.refresh(Request())
auth_token = credentials.token

# 2. Configure the BigLake REST Catalog
REGION = "us-central1"
PROJECT_ID = "trim-strength-477307-h0"
CATALOG_NAME = "learnbiglakeiceberg20"
GCS_BUCKET = "gs://learnbiglakeiceberg20"
WAREHOUSE_PATH = GCS_BUCKET

catalog = load_catalog(
    CATALOG_NAME,
    **{
        "type": "rest",
        "uri": f"https://biglake.googleapis.com/iceberg/v1/restcatalog",
        "token": auth_token,
        "warehouse": WAREHOUSE_PATH,
        "header.X-Goog-User-Project": "trim-strength-477307-h0",
        "header.Authorization": f"Bearer {auth_token}",
        "header.X-Iceberg-Access-Delegation": "vended-credentials",
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
    }
)

# 3. Define Schema & Table
schema = Schema(
    NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
    NestedField(field_id=2, name="data", field_type=StringType(), required=False),
)

namespace = "iceberg_rest_catalog_namespace20"
table_identifier = f"{namespace}.customer_details_pyiceberg_pyspark_created"

# Create namespace and table if they don't exist
try:
    if namespace not in catalog.list_namespaces():
        catalog.create_namespace_if_not_exists(
            namespace,
            properties={"location": f"{WAREHOUSE_PATH}/{namespace}"}
        )
except Exception as e:
    print(f"Warning: Could not create namespace: {e}. Proceeding with table creation...")

try:
    table = catalog.load_table(table_identifier)
except Exception:
    table = catalog.create_table_if_not_exists(
        identifier=table_identifier,
        schema=schema,
        location=f"{WAREHOUSE_PATH}/{namespace}/customer_details_pyiceberg_pyspark_created"
    )

arrow_schema = pa.schema([
    pa.field("id", pa.int32(), nullable=False),
    pa.field("data", pa.string(), nullable=True)
])

# 2. Create the data using this schema
data_rows = [
    {"id": 1, "data": "Hello BigLake"},
    {"id": 2, "data": "Type mismatch fixed"}
]

data = pa.Table.from_pylist(data_rows, schema=arrow_schema)


table.append(data)
print(f"Successfully wrote {len(data)} records to {table_identifier}")

result_arrow = table.scan().to_arrow()
print(result_arrow.to_pandas())


spark.stop()