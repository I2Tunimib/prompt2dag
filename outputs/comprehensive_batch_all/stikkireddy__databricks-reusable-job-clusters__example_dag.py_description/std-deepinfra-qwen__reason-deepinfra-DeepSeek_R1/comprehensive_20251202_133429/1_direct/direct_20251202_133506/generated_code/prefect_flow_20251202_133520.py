from prefect import flow, task
from prefect_databricks import DatabricksCredentials
from prefect_databricks.flows import DatabricksSubmitRun

# Databricks credentials
databricks_credentials = DatabricksCredentials.load("databricks_default")

# Databricks cluster ID
existing_cluster_id = "your_existing_cluster_id"

# Databricks notebook path
notebook_path = "/Users/your_username/helloworld"

# Branch function
def branch_func():
    return "dummy_task_3"

# Tasks
@task
def start_task():
    """Dummy start task."""
    return "Pipeline started"

@task
def dummy_task_1():
    """Dummy task 1."""
    return "Dummy task 1 completed"

@task
def dummy_task_2():
    """Dummy task 2."""
    return "Dummy task 2 completed"

@task
def dummy_task_3():
    """Dummy task 3."""
    return "Dummy task 3 completed"

@task
def end_task():
    """Dummy end task."""
    return "Pipeline completed"

@task
def branch_task():
    """Branch task to determine next step."""
    return branch_func()

# Databricks notebook task
@task
def notebook_task(existing_cluster_id, notebook_path):
    """Run Databricks notebook task."""
    job = {
        "existing_cluster_id": existing_cluster_id,
        "notebook_task": {
            "notebook_path": notebook_path
        }
    }
    return DatabricksSubmitRun(databricks_credentials=databricks_credentials, job=job).run()

# Flow
@flow
def databricks_pipeline():
    """Orchestrates the Databricks pipeline with conditional branching and parallel execution."""
    start = start_task()
    notebook_result = notebook_task.submit(existing_cluster_id, notebook_path)
    dummy_1 = dummy_task_1.submit()
    
    branch_result = branch_task.submit()
    
    dummy_3 = dummy_task_3.submit()
    notebook_2 = notebook_task.submit(existing_cluster_id, notebook_path)
    
    dummy_2 = dummy_task_2.submit(notebook_2)
    
    end = end_task.submit(dummy_3, dummy_2)
    
    return end

if __name__ == '__main__':
    databricks_pipeline()