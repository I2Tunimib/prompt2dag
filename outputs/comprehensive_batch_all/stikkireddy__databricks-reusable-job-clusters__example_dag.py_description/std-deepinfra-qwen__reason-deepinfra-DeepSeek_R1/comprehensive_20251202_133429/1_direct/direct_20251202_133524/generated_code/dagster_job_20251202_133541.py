from dagster import op, job, resource, execute_in_process

# Simplified resource for Databricks connection
@resource
def databricks_resource():
    return {
        "existing_cluster_id": "your-cluster-id",
        "databricks_host": "your-databricks-host",
        "databricks_token": "your-databricks-token"
    }

# Dummy start task
@op
def start_task(context):
    """Dummy start task to mark the beginning of the pipeline."""
    context.log.info("Pipeline started")

# Execute notebook on Databricks cluster
@op(required_resource_keys={"databricks"})
def notebook_task(context):
    """Execute a Databricks notebook on an existing cluster."""
    cluster_id = context.resources.databricks["existing_cluster_id"]
    context.log.info(f"Running notebook on cluster {cluster_id}")

# Dummy task 1
@op
def dummy_task_1(context):
    """Dummy task 1."""
    context.log.info("Dummy task 1 completed")

# Branch task to determine next step
@op
def branch_task(context):
    """Branch task to determine the next step in the pipeline."""
    context.log.info("Evaluating branch condition")
    return "dummy_task_3"  # Always select dummy_task_3

# Dummy task 3
@op
def dummy_task_3(context):
    """Dummy task 3."""
    context.log.info("Dummy task 3 completed")

# Execute notebook on Databricks cluster (second instance)
@op(required_resource_keys={"databricks"})
def notebook_task_2(context):
    """Execute a Databricks notebook on an existing cluster (second instance)."""
    cluster_id = context.resources.databricks["existing_cluster_id"]
    context.log.info(f"Running notebook on cluster {cluster_id}")

# Dummy task 2
@op
def dummy_task_2(context):
    """Dummy task 2."""
    context.log.info("Dummy task 2 completed")

# Final end task
@op
def end_task(context):
    """Final end task to mark the completion of the pipeline."""
    context.log.info("Pipeline completed")

# Define the job
@job(resource_defs={"databricks": databricks_resource})
def databricks_pipeline():
    start = start_task()
    notebook = notebook_task(start)
    dummy1 = dummy_task_1(notebook)
    branch = branch_task(dummy1)
    dummy3 = dummy_task_3(branch)
    notebook2 = notebook_task_2(branch)
    dummy2 = dummy_task_2(notebook2)
    end_task(dummy3, dummy2)

# Execute the job
if __name__ == '__main__':
    result = databricks_pipeline.execute_in_process()