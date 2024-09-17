import gradio as gr
import numpy as np
import json
import os
import logging
#from pyspark.sql.functions import expr
import sys


# Add the parent directory to the Python path
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

# Import custom modules
from resource_recommender import determine_workload_type, recommend_resources
from spark_code_generator import generate_spark_code
from utils import parse_list_input, save_parameters, load_parameters, list_parameter_files


# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def update_workload_type(num_tables, table_sizes, join_complexities, transformation_complexities, data_skews):
    """
    Update the workload type based on input parameters.
    """
    num_tables = int(num_tables)
    table_sizes = parse_list_input(table_sizes)
    join_complexities = parse_list_input(join_complexities)
    transformation_complexities = parse_list_input(transformation_complexities)
    data_skews = parse_list_input(data_skews)
    
    return determine_workload_type(num_tables, table_sizes, join_complexities, transformation_complexities, data_skews)

def update_tables(tables):
    """
    Update table choices in dropdowns based on the current tables.
    """
    if isinstance(tables, str):
        tables = json.loads(tables)
    table_aliases = [table['alias'] for table in tables]
    return [gr.update(choices=table_aliases) for _ in range(8)]

# Initialize join components
join_components = []
for i in range(4):
    with gr.Row():
        join_components.extend([
            gr.Dropdown(label=f"Left Table/Subquery {i+1}", choices=[]),
            gr.Dropdown(label=f"Join Type {i+1}", choices=['inner', 'left', 'right', 'full', 'subquery', 'self']),
            gr.Dropdown(label=f"Right Table {i+1}", choices=[]),
            gr.Textbox(label=f"Join Condition {i+1}", placeholder="e.g., table1.id == table2.id"),
            gr.Textbox(label=f"Subquery {i+1}", placeholder="SELECT * FROM ..."),
            gr.Textbox(label=f"Left Alias {i+1} (for self-join)", placeholder="t1"),
            gr.Textbox(label=f"Right Alias {i+1} (for self-join)", placeholder="t2")
        ])

def update_joins(tables, joins):
    """
    Update join components based on the current tables and joins.
    """
    if isinstance(tables, str):
        tables = json.loads(tables)
    if isinstance(joins, str):
        joins = json.loads(joins)
    table_aliases = [table['alias'] for table in tables]
    
    join_components = []
    for i in range(4):  # Always return 4 sets of join components
        if i < len(joins):
            join = joins[i]
            join_components.extend([
                gr.update(choices=table_aliases + ['subquery', 'self'], value=join.get('left_table', '')),
                gr.update(choices=['inner', 'left', 'right', 'full', 'subquery', 'self'], value=join.get('type', 'inner')),
                gr.update(choices=table_aliases, value=join.get('right_table', '')),
                gr.update(value=join.get('conditions', '')),
                gr.update(value=join.get('subquery', '')),
                gr.update(value=join.get('left_alias', '')),
                gr.update(value=join.get('right_alias', ''))
            ])
        else:
            join_components.extend([
                gr.update(choices=table_aliases + ['subquery', 'self']),
                gr.update(choices=['inner', 'left', 'right', 'full', 'subquery', 'self']),
                gr.update(choices=table_aliases),
                gr.update(),
                gr.update(),
                gr.update(),
                gr.update()
            ])

    return join_components

# Create the Gradio interface
with gr.Blocks() as iface:
    gr.Markdown("# Enhanced EMR Job Resource Recommender")
    gr.Markdown("Enter your EMR job parameters to get comprehensive recommendations for cluster resources and configurations.")
    
    # Input components for job parameters
    with gr.Row():
        num_tables = gr.Slider(1, 20, step=1, value=1, label="Number of Tables")
        table_sizes = gr.Textbox(label="Table Sizes (GB, comma-separated)", value="10", placeholder="e.g., 10,20,30")
    
    with gr.Row():
        join_complexities = gr.Textbox(label="Join Complexities (1-10, comma-separated)", value="5", placeholder="e.g., 5,7,3")
        transformation_complexities = gr.Textbox(label="Transformation Complexities (1-10, comma-separated)", value="5", placeholder="e.g., 4,6,8")
        data_skews = gr.Textbox(label="Data Skews (1-10, comma-separated)", value="5", placeholder="e.g., 2,5,9")
        
    gr.Markdown("Data Skew Explanation: 1 = Evenly distributed, 10 = Highly skewed")
    
    # Checkbox options for job configuration
    with gr.Row():
        use_spot_instances = gr.Checkbox(label="Use Spot Instances")
        sla_requirements = gr.Checkbox(label="Strict SLA Requirements")
        enable_auto_termination = gr.Checkbox(label="Enable Auto-termination")
        
    with gr.Row():
        recency = gr.Slider(0, 10, step=1, value=5, label="Data Recency (0-10)")
        frequency = gr.Slider(0, 10, step=1, value=5, label="Update Frequency (0-10)")
    
    # Workload type selection
    workload_type = gr.Radio(["General", "ML/High Memory", "Streaming/ELT", "Data Caching/Analysis"], value="General", label="Workload Type")
    
    submit_button = gr.Button("Get Recommendations")
    output = gr.Textbox(label="Recommendations")

    # Spark Code Generation tab
    with gr.Tab("Spark Code Generation"):
        gr.Markdown("Define tables, joins, predicates, and transformations to generate complex Spark code")
        
        # Table definition components
        tables_json = gr.JSON(label="Tables", value=[{"name": "", "schema": "", "alias": "", "predicate": ""}])
        
        with gr.Row():
            add_table_name = gr.Textbox(label="Table Name")
            add_table_schema = gr.Textbox(label="Schema")
            add_table_alias = gr.Textbox(label="Alias")
            add_table_predicate = gr.Textbox(label="Table Predicate")
            add_table_button = gr.Button("Add Table")
        
        # Join definition components
        joins_json = gr.JSON(label="Joins", value=[])
        
        join_components = []
        for i in range(4):
            with gr.Row():
                if i == 0:
                    join_components.extend([
                        gr.Dropdown(label=f"Left Table/Subquery {i+1}", choices=[], interactive=True),
                        gr.Dropdown(label=f"Join Type {i+1}", choices=['inner', 'left', 'right', 'full', 'subquery', 'self'], interactive=True),
                        gr.Dropdown(label=f"Right Table {i+1}", choices=[], interactive=True),
                        gr.Textbox(label=f"Join Condition {i+1}", placeholder="e.g., table1.id == table2.id"),
                        gr.Textbox(label=f"Subquery {i+1}", placeholder="SELECT * FROM ..."),
                        gr.Textbox(label=f"Left Alias {i+1} (for self-join)", placeholder="t1"),
                        gr.Textbox(label=f"Right Alias {i+1} (for self-join)", placeholder="t2")
                    ])
                else:
                    join_components.extend([
                        gr.Dropdown(label=f"Left Table/Subquery {i+1}", choices=[], interactive=False),
                        gr.Dropdown(label=f"Join Type {i+1}", choices=['inner', 'left', 'right', 'full', 'subquery', 'self'], interactive=False),
                        gr.Dropdown(label=f"Right Table {i+1}", choices=[], interactive=False),
                        gr.Textbox(label=f"Join Condition {i+1}", interactive=False),
                        gr.Textbox(label=f"Subquery {i+1}", interactive=False),
                        gr.Textbox(label=f"Left Alias {i+1} (for self-join)", interactive=False),
                        gr.Textbox(label=f"Right Alias {i+1} (for self-join)", interactive=False)
                    ])
                    

        add_join_button = gr.Button("Add Join")
        
        # Predicate and transformation components
        predicates = gr.Textbox(label="Global Predicates", placeholder="e.g., column > 5 AND other_column != 'value'")
        
        transformations_json = gr.JSON(label="Transformations", value=[])
        
        with gr.Row():
            add_transformation_output = gr.Textbox(label="Output Column")
            add_transformation_expression = gr.Textbox(label="Transformation Expression")
            add_transformation_button = gr.Button("Add Transformation")
            
        # Partition and output components
        with gr.Row():
            partition_columns = gr.Textbox(label="Partition Columns (comma-separated)")
            partition_values = gr.Textbox(label="Partition Values (comma-separated)")
        
        with gr.Row():
            output_table = gr.Textbox(label="Output Table Name")
            output_schema = gr.Textbox(label="Output Schema")
            write_mode = gr.Dropdown(label="Write Mode", choices=["overwrite", "append"], value="overwrite")
        
        # Code generation and parameter management components
        generate_code_button = gr.Button("Generate Spark Code")
        spark_code_output = gr.Code(language="python", label="Generated Spark Code")
        
        with gr.Row():
            job_name = gr.Textbox(label="Job Name (optional)")
            save_button = gr.Button("Save Parameters")
            load_dropdown = gr.Dropdown(label="Load Parameters", choices=list_parameter_files())
            load_button = gr.Button("Load Selected Parameters")
            status_message = gr.Textbox(label="Status")

        with gr.Row():
            clear_tables_button = gr.Button("Clear Tables")
            clear_joins_button = gr.Button("Clear Joins")

    # Dynamic update of workload type
    num_tables.change(determine_workload_type, inputs=[num_tables, table_sizes, join_complexities, transformation_complexities, data_skews, recency, frequency], outputs=workload_type)
    table_sizes.change(determine_workload_type, inputs=[num_tables, table_sizes, join_complexities, transformation_complexities, data_skews, recency, frequency], outputs=workload_type)
    join_complexities.change(determine_workload_type, inputs=[num_tables, table_sizes, join_complexities, transformation_complexities, data_skews, recency, frequency], outputs=workload_type)
    transformation_complexities.change(determine_workload_type, inputs=[num_tables, table_sizes, join_complexities, transformation_complexities, data_skews, recency, frequency], outputs=workload_type)
    data_skews.change(determine_workload_type, inputs=[num_tables, table_sizes, join_complexities, transformation_complexities, data_skews, recency, frequency], outputs=workload_type)
    recency.change(determine_workload_type, inputs=[num_tables, table_sizes, join_complexities, transformation_complexities, data_skews, recency, frequency], outputs=workload_type)
    frequency.change(determine_workload_type, inputs=[num_tables, table_sizes, join_complexities, transformation_complexities, data_skews, recency, frequency], outputs=workload_type)


    # Submit button action
    submit_button.click(
        recommend_resources,
        inputs=[num_tables, table_sizes, join_complexities, transformation_complexities, 
                data_skews, use_spot_instances, sla_requirements, 
                workload_type, enable_auto_termination, recency, frequency],
        outputs=output
    )

    # Add table button action
    def add_table(tables, name, schema, alias, predicate):
        """
        Add a new table to the tables JSON.
        """
        if isinstance(tables, str):
            tables = json.loads(tables)
        tables.append({"name": name, "schema": schema, "alias": alias, "predicate": predicate})
        return json.dumps(tables), "", "", "", ""

    add_table_button.click(
        add_table,
        inputs=[tables_json, add_table_name, add_table_schema, add_table_alias, add_table_predicate],
        outputs=[tables_json, add_table_name, add_table_schema, add_table_alias, add_table_predicate]
    )

    # Update dropdowns when tables are added
    tables_json.change(
        update_tables,
        inputs=[tables_json],
        outputs=[comp for comp in join_components if isinstance(comp, gr.Dropdown) and comp.label.startswith(("Left Table/Subquery", "Right Table"))] # Update all left and right table dropdowns
    )
    
    joins_json.change(
        update_joins,
        inputs=[tables_json, joins_json],
        outputs=join_components
    )

    # Add join button action
    def add_join(joins, *join_inputs):
        """
        Add a new join to the joins JSON.
        """
        if isinstance(joins, str):
            joins = json.loads(joins)
        new_join = {
            "left_table": join_inputs[0],
            "type": join_inputs[1],
            "right_table": join_inputs[2],
            "conditions": join_inputs[3]
        }
        if new_join not in joins:
            joins.append(new_join)
        return json.dumps(joins)

    add_join_button.click(
        add_join,
        inputs=[joins_json] + join_components[:4],  # Only use the first set of join inputs
        outputs=[joins_json]
    )
    
    # Add transformation button action
    def add_transformation(transformations, output_column, expression):
        """
        Add a new transformation to the transformations JSON.
        """
        if isinstance(transformations, str):
            transformations = json.loads(transformations)
        transformations.append({"output_column": output_column, "expression": expression})
        return json.dumps(transformations), "", ""

    add_transformation_button.click(
        add_transformation,
        inputs=[transformations_json, add_transformation_output, add_transformation_expression],
        outputs=[transformations_json, add_transformation_output, add_transformation_expression]
    )

    # Generate code button action
    
    def generate_code_with_recommendations(tables, joins, predicates, recommendations, output_table, output_schema, transformations, write_mode, partition_columns, partition_values):
        """
        Generate Spark code based on the provided parameters and recommendations.
        """
        logging.debug(f"generate_code_with_recommendations inputs: tables={tables}, joins={joins}, predicates={predicates}, recommendations={recommendations}")
        
        if isinstance(tables, str):
            tables = json.loads(tables)
        if isinstance(joins, str):
            joins = json.loads(joins)
        if isinstance(transformations, str):
            transformations = json.loads(transformations)
        
        # Handle recommendations parsing
        if isinstance(recommendations, str):
            try:
                recommendations = json.loads(recommendations)
            except json.JSONDecodeError:
                # If recommendations is not valid JSON, extract spark configs
                spark_configs = []
                for line in recommendations.split('\n'):
                    if line.strip().startswith('spark.'):
                        spark_configs.append(line.strip())
                recommendations = {"spark_configs": spark_configs}

        spark_configs = "\n".join([f"spark = spark.config('{config.split('=')[0].strip()}', '{config.split('=')[1].strip()}')" for config in recommendations.get("spark_configs", [])])
        return generate_spark_code(json.dumps(tables), json.dumps(joins), predicates, spark_configs, output_table, output_schema, json.dumps(transformations), write_mode, partition_columns, partition_values)

    generate_code_button.click(
        generate_code_with_recommendations,
        inputs=[tables_json, joins_json, predicates, output, output_table, output_schema, transformations_json, write_mode, partition_columns, partition_values],
        outputs=[spark_code_output]
    )
    
    # Add new button actions
    save_button.click(
        save_parameters,
        inputs=[tables_json, joins_json, predicates, output_table,  output_schema, transformations_json, job_name, write_mode, partition_columns, partition_values],
        outputs=[status_message]
    )

    load_button.click(
        load_parameters,
        inputs=[load_dropdown],
        outputs=[tables_json, joins_json, predicates, transformations_json, output_table, output_schema, status_message, write_mode, partition_columns, partition_values]
    )
    
    # Update the list of saved parameter files
    save_button.click(lambda: gr.update(choices=list_parameter_files()), outputs=[load_dropdown])

    def clear_tables():
        """
        Clear all tables from the tables JSON.
        """
        return json.dumps([])

    clear_tables_button.click(
        clear_tables,
        outputs=[tables_json]
    )

    def clear_joins():
        """
        Clear all joins from the joins JSON.
        """
        return json.dumps([])

    clear_joins_button.click(
        clear_joins,
        outputs=[joins_json]
    )
    
    # Update dropdowns when parameters are loaded
    load_button.click(
        update_tables,
        inputs=[tables_json],
        outputs=[comp for comp in join_components if isinstance(comp, gr.Dropdown) and comp.label.startswith(("Left Table/Subquery", "Right Table"))]
    )

    load_button.click(
        update_joins,
        inputs=[tables_json, joins_json],
        outputs=join_components
    )


# Launch the application
if __name__ == "__main__":
    iface.launch()
