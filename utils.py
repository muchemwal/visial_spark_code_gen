import json
import os
from datetime import datetime

def parse_list_input(input_str, default_value=1):
    try:
        return [float(x.strip()) for x in input_str.split(',') if x.strip()]
    except ValueError:
        return [default_value]

# Add new functions for saving and loading parameters
def save_parameters(tables, joins, predicates, output_table, output_schema, transformations, job_name, write_mode, partition_columns, partition_values):
    params = {
        "tables": tables,
        "joins": joins,
        "predicates": predicates,
        "transformations": transformations,
        "output_table": output_table,
        "output_schema": output_schema,
        "write_mode": write_mode,
        "partition_columns": partition_columns,
        "partition_values": partition_values
    }
    if job_name:
        filename = f"{job_name}.json"
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"saved_parameters_{timestamp}.json"
    
    with open(filename, "w") as f:
        json.dump(params, f)
    return f"Parameters saved successfully as {filename}"

# Modify the load_parameters function to handle multiple save files
def load_parameters(filename):
    try:
        with open(filename, "r") as f:
            params = json.load(f)
        return (
            params["tables"],
            params["joins"],
            params["predicates"],
            params["transformations"],
            params["output_table"],
            params["output_schema"],
            params["write_mode"],
            params["partition_columns"],
            params["partition_values"],
            f"Parameters loaded successfully from {filename}"
        )
    except FileNotFoundError:
        return None, None, None, None, None, None, None, None, None, f"File {filename} not found."

# Function to list saved parameter files
def list_parameter_files():
    return [f for f in os.listdir() if f.endswith('.json') and (f.startswith('saved_parameters_') or f.endswith('_job.json'))]
