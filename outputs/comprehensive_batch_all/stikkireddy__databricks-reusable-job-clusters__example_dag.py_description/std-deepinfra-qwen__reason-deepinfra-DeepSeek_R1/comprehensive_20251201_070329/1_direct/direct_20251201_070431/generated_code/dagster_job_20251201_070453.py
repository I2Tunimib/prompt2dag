from dagster import op, job, ResourceDefinition, execute_in_process

# Simplified resource example for Databricks connection
databricks_resource = ResourceDefinition.hardcoded_resource(
    {"existing_cluster_id": "your_cluster_id", "notebook_path": "/Users/your_user/helloworld"}
)

@op(required_resource_keys={"databricks"})
def start_task(context):
    """Dummy start task as entry point."""
    context.log.info("Starting the pipeline.")
    return "start"

@op(required_resource_keys={"databricks"})
def notebook_task(context):
    """Execute helloworld notebook on existing Databricks cluster."""
    cluster_id = context.resources.databricks["existing_cluster_id"]
    notebook_path = context.resources.databricks["notebook_path"]
    context.log.info(f"Running notebook {notebook_path} on cluster {cluster_id}")
    return "notebook_task_completed"

@op
def dummy_task_1(context):
    """Dummy task 1."""
    context.log.info("Executing dummy task 1.")
    return "dummy_task_1_completed"

@op
def branch_func(context):
    """Branch function to determine next step."""
    context.log.info("Evaluating branch condition.")
    return "dummy_task_3"

@op
def dummy_task_3(context):
    """Dummy task 3."""
    context.log.info("Executing dummy task 3.")
    return "dummy_task_3_completed"

@op(required_resource_keys={"databricks"})
def notebook_task_2(context):
    """Execute helloworld notebook on existing Databricks cluster."""
    cluster_id = context.resources.databricks["existing_cluster_id"]
    notebook_path = context.resources.databricks["notebook_path"]
    context.log.info(f"Running notebook {notebook_path} on cluster {cluster_id}")
    return "notebook_task_2_completed"

@op
def dummy_task_2(context):
    """Dummy task 2."""
    context.log.info("Executing dummy task 2.")
    return "dummy_task_2_completed"

@op
def end_task(context):
    """Final end task to mark pipeline completion."""
    context.log.info("Pipeline completed.")
    return "end_task_completed"

@job(resource_defs={"databricks": databricks_resource})
def your_job():
    start = start_task()
    notebook = notebook_task(start)
    dummy1 = dummy_task_1(notebook)
    branch = branch_func(dummy1)
    dummy3 = dummy_task_3(branch)
    notebook2 = notebook_task_2(branch)
    dummy2 = dummy_task_2(notebook2)
    end_task(dummy3, dummy2)

if __name__ == '__main__':
    result = your_job.execute_in_process()