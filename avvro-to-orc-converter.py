import fastavro
import pyarrow as pa
import pyarrow.orc as orc

# Step 1: Read the Avro file
def read_avro(file_path):
    with open(file_path, "rb") as avro_file:
        avro_reader = fastavro.reader(avro_file)
        records = [record for record in avro_reader]
    return records

# Step 2: Convert Avro records to a PyArrow Table
def convert_to_arrow_table(records):
    # Convert records to a PyArrow Table
    table = pa.Table.from_pylist(records)
    return table

# Step 3: Write the PyArrow Table to an ORC file
def write_orc(table, output_path):
    orc.write_table(table, output_path)

# Main function
def avro_to_orc(avro_file_path, orc_file_path):
    # Read Avro file
    records = read_avro(avro_file_path)
    
    # Convert to PyArrow Table
    table = convert_to_arrow_table(records)
    
    # Write to ORC file
    write_orc(table, orc_file_path)
    print(f"ORC file saved to: {orc_file_path}")

# Example usage
avro_file_path = "input.avro"  # Path to your Avro file
orc_file_path = "output.orc"   # Path to save the ORC file
avro_to_orc(avro_file_path, orc_file_path)
