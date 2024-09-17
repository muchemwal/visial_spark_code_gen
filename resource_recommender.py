import numpy as np
from utils import parse_list_input


def determine_workload_type(num_tables, table_sizes, join_complexities, transformation_complexities, data_skews, recency, frequency):
    num_tables = int(num_tables)
    table_sizes = parse_list_input(table_sizes)
    join_complexities = parse_list_input(join_complexities)
    transformation_complexities = parse_list_input(transformation_complexities)
    data_skews = parse_list_input(data_skews)
    
    total_size = sum(table_sizes)
    avg_size = total_size / len(table_sizes)
    max_size = max(table_sizes)
    size_variance = np.var(table_sizes)
    
    avg_join_complexity = np.mean(join_complexities)
    max_join_complexity = max(join_complexities)
    
    avg_transformation_complexity = np.mean(transformation_complexities)
    max_transformation_complexity = max(transformation_complexities)
    
    avg_data_skew = np.mean(data_skews)
    max_data_skew = max(data_skews)

    # Initialize scores for each workload type
    scores = {
        "General": 0,
        "ML/High Memory": 0,
        "Streaming/ELT": 0,
        "Data Caching/Analysis": 0
    }

    # Scoring based on number of tables
    if num_tables <= 5:
        scores["ML/High Memory"] += 2
        scores["General"] += 1
    elif 5 < num_tables <= 15:
        scores["General"] += 2
        scores["Data Caching/Analysis"] += 1
    else:
        scores["Streaming/ELT"] += 2
        scores["Data Caching/Analysis"] += 1

    # Scoring based on data sizes
    if avg_size > 500:
        scores["ML/High Memory"] += 2
    elif 100 < avg_size <= 500:
        scores["Data Caching/Analysis"] += 2
    elif avg_size <= 100:
        scores["Streaming/ELT"] += 1
        scores["General"] += 1

    if max_size > 1000:
        scores["ML/High Memory"] += 1
    if size_variance > 10000:  # High variance in table sizes
        scores["Data Caching/Analysis"] += 2

    # Scoring based on join complexity
    if avg_join_complexity > 7:
        scores["Data Caching/Analysis"] += 2
    elif 4 <= avg_join_complexity <= 7:
        scores["General"] += 1
        scores["Streaming/ELT"] += 1
    if max_join_complexity > 8:
        scores["Data Caching/Analysis"] += 1

    # Scoring based on transformation complexity
    if avg_transformation_complexity > 7:
        scores["ML/High Memory"] += 2
    elif 4 <= avg_transformation_complexity <= 7:
        scores["Streaming/ELT"] += 2
    if max_transformation_complexity > 8:
        scores["ML/High Memory"] += 1

    # Scoring based on data skew
    if avg_data_skew > 7:
        scores["Data Caching/Analysis"] += 2
    elif 4 <= avg_data_skew <= 7:
        scores["Streaming/ELT"] += 1
    if max_data_skew > 8:
        scores["Data Caching/Analysis"] += 1

    # Scoring based on recency
    if recency > 7:
        scores["Streaming/ELT"] += 2
    elif 4 <= recency <= 7:
        scores["Data Caching/Analysis"] += 1

    # Scoring based on frequency
    if frequency > 7:
        scores["Streaming/ELT"] += 2
    elif 4 <= frequency <= 7:
        scores["Data Caching/Analysis"] += 1

    # Determine the workload type with the highest score
    workload_type = max(scores, key=scores.get)

    # If the highest score is not significantly higher than others, consider it a General workload
    if scores[workload_type] - max(score for type, score in scores.items() if type != workload_type) <= 2:
        workload_type = "General"

    return workload_type


def recommend_resources(num_tables, table_sizes, join_complexities, transformation_complexities, 
                        data_skews, use_spot_instances, sla_requirements, 
                        workload_type, enable_auto_termination, recency, frequency):
    # Input validation and parsing
    try:
        num_tables = int(num_tables)
        table_sizes = parse_list_input(table_sizes)
        join_complexities = parse_list_input(join_complexities)
        transformation_complexities = parse_list_input(transformation_complexities)
        data_skews = parse_list_input(data_skews)
    except ValueError as e:
        return f"Error in input: {str(e)}. Please check your inputs and try again."

    # Ensure all lists have the same length as num_tables
    table_sizes = table_sizes * num_tables if len(table_sizes) < num_tables else table_sizes[:num_tables]
    join_complexities = join_complexities * num_tables if len(join_complexities) < num_tables else join_complexities[:num_tables]
    transformation_complexities = transformation_complexities * num_tables if len(transformation_complexities) < num_tables else transformation_complexities[:num_tables]
    data_skews = data_skews * num_tables if len(data_skews) < num_tables else data_skews[:num_tables]

    total_data_size = sum(table_sizes)




    # Initialize recommendations
    recommendations = []
    
    # Calculate total cores needed based on data size and complexity
    total_cores = max(2, int(np.ceil(total_data_size / 128)))  # Assuming 128MB per core
    
    # Determine instance types based on workload
    if workload_type == "ML/High Memory":
        instance_type = "r5.xlarge"  # Memory optimized
    elif workload_type == "Streaming/ELT":
        instance_type = "c5.xlarge"  # Compute optimized
    elif workload_type == "Data Caching/Analysis":
        instance_type = "i3.xlarge"  # Storage optimized
    else:
        instance_type = "m5.xlarge"  # General purpose

        # Calculate instance details
    instance_cores = 4  # Assuming 4 cores per instance, adjust as needed
    instance_memory = 16  # Assuming 16 GB memory per instance, adjust as needed

    # Calculate number of worker nodes
    workers = max(2, int(np.ceil(total_cores / instance_cores)))  # Assuming 4 cores per instance

    # Spot instance recommendations
    spot_percentage = 0
    if use_spot_instances:
        spot_percentage = 80 if not sla_requirements else 20
        recommended_spot_workers = int(workers * spot_percentage / 100)
        recommended_on_demand_workers = workers - recommended_spot_workers

    # Auto-scaling configurations
    min_instances = max(1, workers - 2)  # Minimum instances is either 1 or 2 less than recommended
    max_instances = workers * 2  # Maximum instances is double the recommended
    
    scale_out_adjustment = 1  # Add 1 instance at a time
    scale_in_adjustment = -1  # Remove 1 instance at a time
    
    cooldown_period = 300  # 5 minutes cooldown between scaling activities
    
    # Safeguards against long-running jobs
    max_execution_time = 6 * 60 * 60  # 6 hours in seconds

    # EMR step to monitor and terminate long-running jobs
    termination_step = {
        "Name": "Monitor and Terminate Long-Running Jobs",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "bash", "-c", 
                f"""
                while true; do
                    current_time=$(date +%s)
                    for app in $(yarn application -list | awk '{{print $1}}' | tail -n +3); do
                        start_time=$(yarn application -status $app | grep 'Start-Time' | awk '{{print $3}}')
                        elapsed_time=$((current_time - start_time/1000))
                        if [ $elapsed_time -gt {max_execution_time} ]; then
                            yarn application -kill $app
                            echo "Terminated long-running job: $app"
                        fi
                    done
                    sleep 300  # Check every 5 minutes
                done
                """
            ]
        }
    }

    
    # Adjust for spot instances
    if use_spot_instances and not sla_requirements:
        recommendations.append("Using spot instances for workers. Consider a mix of on-demand and spot instances for reliability.")
    elif sla_requirements:
        recommendations.append("Using on-demand instances due to SLA requirements.")
    
    # Auto-termination
    if enable_auto_termination:
        recommendations.append("Enabled auto-termination. Cluster will terminate after 15 minutes of inactivity.")
    
    # Spark configurations
    spark_configs = [
        f"spark.sql.shuffle.partitions = {2 * total_cores}",
        "spark.sql.adaptive.enabled = true",
        "spark.sql.adaptive.coalescePartitions.enabled = true",
        "spark.sql.adaptive.skewJoin.enabled = true",
        f"spark.sql.adaptive.skewJoin.skewedPartitionFactor = {max(5, int(np.max(data_skews)))}",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 268435456",  # 256MB
        "spark.sql.adaptive.localShuffleReader.enabled = true",
        "spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled = true",
    ]
    
    # Additional recommendations based on input
    if np.max(join_complexities) > 7:
        recommendations.append("Consider using broadcast joins for smaller tables.")
    
    if np.max(data_skews) > 7:
        recommendations.append("High data skew detected. Consider salting keys for better distribution.")
    
    if np.max(transformation_complexities) > 7:
        recommendations.append("Complex transformations detected. Consider caching intermediate results.")

    # Update recommendations list
    recommendations.append(f"Configured auto-scaling with min instances: {min_instances}, max instances: {max_instances}")
    recommendations.append(f"Scale-out adjustment: +{scale_out_adjustment} instance, Scale-in adjustment: {scale_in_adjustment} instance")
    recommendations.append(f"Auto-scaling cooldown period: {cooldown_period} seconds")
    recommendations.append(f"Added safeguard to terminate jobs running longer than {max_execution_time/3600} hours")

    # Display Spark Configurations with newline preserved
    spark_configs_display = "\n".join(spark_configs)

    recommendations_display = "\n".join(recommendations)
    
    # Compile final recommendations
    final_recommendation = f"""
    Detected Workload Type: {workload_type}
    
    EMR Cluster Recommendations:
    - Master Node: 1 x {instance_type}
    - Worker Nodes: {workers} x {instance_type}
    - Total Cores: {total_cores}
    - Cores per Instance: {instance_cores}
    - Memory per Instance: {instance_memory} GB

    Auto-scaling Configuration:
    - Minimum Instances: {min_instances}
    - Maximum Instances: {max_instances}
    - Scale-out Adjustment: +{scale_out_adjustment} instance
    - Scale-in Adjustment: {scale_in_adjustment} instance
    - Cooldown Period: {cooldown_period} seconds

    Spot Instance Configuration:
    - Spot Percentage: {spot_percentage}%
    - Recommended Spot Instances: {recommended_spot_workers if use_spot_instances else 'N/A'}
    - Recommended On-Demand Instances: {recommended_on_demand_workers if use_spot_instances else workers}

    Safeguards:
    - Maximum Job Execution Time: {max_execution_time/3600} hours
    - Long-running Job Termination: Enabled
    
    Spark Configurations:
    {spark_configs_display}
    
    Additional Recommendations:
    {recommendations_display}
    
    Note: These recommendations are based on general guidelines. Always test and adjust based on your specific workload and performance requirements.
    """
    
    return final_recommendation