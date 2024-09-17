

import json
import logging

def generate_spark_code(tables_json, joins_json, predicates, spark_configs, output_table, output_schema, transformations_json, write_mode, partition_columns, partition_values):
    logging.debug(f"generate_spark_code inputs: tables_json={tables_json}, joins_json={joins_json}, predicates={predicates}, spark_configs={spark_configs}, transformations_json={transformations_json}, write_mode={write_mode}, partition_columns={partition_columns}, partition_values={partition_values}")
    
    tables = json.loads(tables_json)
    joins = json.loads(joins_json)
    transformations = json.loads(transformations_json)
    
    spark_code = f"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
import os

# Initialize Spark session with configurations
spark = SparkSession.builder.appName("GeneratedComplexSparkJob")
{spark_configs}
spark = spark.getOrCreate()

# Read environment variables
github_branch = os.getenv('GITHUB_BRANCH', 'main')
environment = os.getenv('ENVIRONMENT', 'dev')

# Read tables
table_dict = {{}}
"""

    for table in tables:
        spark_code += f"""
table_dict["{table['alias']}"] = spark.read.table("{table['schema']}.{table['name']}")
"""
        if table['predicate']:
            spark_code += f"""
table_dict["{table['alias']}"] = table_dict["{table['alias']}"].filter("{table['predicate']}")
"""

    spark_code += """
# Perform joins
result_df = table_dict["{0}"]
""".format(tables[0]['alias'])

    for join in joins:
        if join['type'] == 'subquery':
            spark_code += f"""
subquery_df = spark.sql('''
    {join['subquery']}
''')
result_df = result_df.join(
    subquery_df,
    {join['conditions']},
    "inner"
)
"""
        elif join['type'] == 'self':
            spark_code += f"""
result_df = result_df.alias("{join['left_alias']}").join(
    result_df.alias("{join['right_alias']}"),
    {join['conditions']},
    "inner"
)
"""
        else:
            spark_code += f"""
result_df = result_df.join(
    table_dict["{join['right_table']}"],
    {join['conditions']},
    "{join['type']}"
)
"""

    if predicates:
        spark_code += f"""
# Apply global predicates
result_df = result_df.filter("{predicates}")
"""

    if transformations:
        spark_code += """
# Apply transformations
"""
        group_by_columns = []
        agg_expressions = []
        select_columns = []
        for transform in transformations:
            if transform['expression'].lower().startswith("group by"):
                group_by_columns.extend([col.strip() for col in transform['expression'].lower().split("group by")[1].split(",")])
            elif any(agg_func in transform['expression'].lower() for agg_func in ['sum(', 'avg(', 'count(', 'max(', 'min(']):
                agg_expressions.append(f"{transform['output_column']} = expr('{transform['expression']}')")
            else:
                select_columns.append(f"{transform['expression']} as {transform['output_column']}")

        if group_by_columns:
            group_by_str = ", ".join([f"'{col}'" for col in group_by_columns])
            spark_code += f"""
result_df = result_df.groupBy({group_by_str})
"""
            if agg_expressions:
                agg_str = ", ".join(agg_expressions)
                spark_code += f"""
result_df = result_df.agg({agg_str})
"""
        elif select_columns:
            select_str = ", ".join(select_columns)
            spark_code += f"""
result_df = result_df.selectExpr({select_str})
"""
        else:
            for transform in transformations:
                spark_code += f"""
result_df = result_df.withColumn("{transform['output_column']}", expr("{transform['expression']}"))
"""

    spark_code += f"""
# Write the result to the output table and show Output
result_df.show()

# Prepare write operation
writer = result_df.write.mode("{write_mode}")

# Add partitioning if specified
if "{partition_columns}" and "{partition_values}":
    partition_cols = [col.strip() for col in "{partition_columns}".split(',')]
    partition_vals = [val.strip() for val in "{partition_values}".split(',')]
    for col, val in zip(partition_cols, partition_vals):
        writer = writer.partitionBy(col, val)

# Write the data
writer.saveAsTable("{output_schema}.{output_table}")

print(f"Data written to {{output_schema}}.{{output_table}} on branch {{github_branch}} in {{environment}} environment")
# Stop the Spark session
spark.stop()
"""
    return spark_code
