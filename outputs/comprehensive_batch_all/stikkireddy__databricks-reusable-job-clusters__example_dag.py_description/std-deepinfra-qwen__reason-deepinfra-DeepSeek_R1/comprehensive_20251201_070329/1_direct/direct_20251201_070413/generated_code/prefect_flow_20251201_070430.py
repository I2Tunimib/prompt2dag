from prefect import flow, task
from prefect_databricks import DatabricksCredentials
from prefect_databricks.tasks import submit_run

# Databricks credentials
databricks_credentials = DatabricksCredentials.load("databricks_default")

# Databricks cluster ID
existing_cluster_id = "your_existing_cluster_id"

# Databricks notebook path
notebook_path = "/Users/your_username/helloworld"

# Databricks job configuration
job_config = {
    "existing_cluster_id": existing_cluster_id,
    "notebook_task": {
        "notebook_path": notebook_path
    },
    "new_cluster": {
        "spark_version": "12.2.x-scala2.12",
        "node_type_id": "n2-highmem-4",
        "spark_conf": {
            "spark.databricks.delta.preview.enabled": "true"
        }
    }
}

@task
def start_task():
    """Dummy start task."""
    return "Pipeline started"

@task
def dummy_task_1():
    """Dummy task 1."""
    return "Dummy task 1 completed"

@task
def branch_func():
    """Branch function to determine next step."""
    return "dummy_task_3"

@task
def dummy_task_3():
    """Dummy task 3."""
    return "Dummy task 3 completed"

@task
def notebook_task_2():
    """Notebook task 2 running helloworld notebook."""
    return submit_run.fn(job_config, databricks_credentials=databricks_credentials)

@task
def dummy_task_2():
    """Dummy task 2."""
    return "Dummy task 2 completed"

@task
def end_task():
    """Final end task."""
    return "Pipeline completed"

@flow
def databricks_pipeline():
    """Orchestrates the Databricks pipeline with conditional branching and parallel execution."""
    start = start_task()
    notebook_result = submit_run.fn(job_config, databricks_credentials=databricks_credentials)
    dummy_1 = dummy_task_1()
    
    branch_result = branch_func()
    
    if branch_result == "dummy_task_3":
        dummy_3 = dummy_task_3.submit()
        notebook_2 = notebook_task_2.submit()
        dummy_2 = dummy_task_2.submit(wait_for=[notebook_2])
        end = end_task.submit(wait_for=[dummy_3, dummy_2])
    else:
        raise ValueError("Invalid branch result")

if __name__ == "__main__":
    databricks_pipeline()