from prefect import flow, task
from prefect_databricks import jobs_submit
from prefect_databricks.models.jobs import NotebookTask, ExistingClusterId
from prefect.blocks.system import Secret
import time
from typing import Any, Dict

# Dummy start task
@task
def start_task() -> str:
    """Pipeline entry point."""
    print("Starting pipeline execution")
    return "started"

# Databricks notebook task
@task
def run_notebook_task(notebook_path: str, cluster_id: str, task_name: str) -> Dict[str, Any]:
    """
    Submit a notebook task to Databricks.
    
    Args:
        notebook_path: Path to the notebook in Databricks workspace
        cluster_id: ID of the existing Databricks cluster
        task_name: Name for this task execution
        
    Returns:
        Job run metadata
    """
    # In a real scenario, you'd load these from Secret blocks
    # databricks_host = Secret.load("databricks-host").get()
    # databricks_token = Secret.load("databricks-token").get()
    
    # For this example, we'll use placeholder values that would be
    # replaced with actual secrets in production
    databricks_host = "https://your-databricks-instance.cloud.databricks.com"
    
    # Submit the notebook job
    result = jobs_submit(
        databricks_instance=databricks_host,
        # authentication would go here - using personal access token
        # access_control_list=None,
        run_name=task_name,
        tasks=[
            {
                "task_key": task_name,
                "notebook_task": NotebookTask(
                    notebook_path=notebook_path,
                    source="WORKSPACE"
                ),
                "existing_cluster_id": cluster_id
            }
        ],
        # For existing cluster, we don't need new_cluster spec
        # But if we wanted to use a job cluster, we'd include:
        # "new_cluster": {
        #     "node_type_id": "n2-highmem-4",
        #     "spark_version": "12.2.x-scala2.12",
        #     "spark_conf": {
        #         "spark.databricks.delta.preview.enabled": "true"
        #     }
        # }
    )
    
    return result

# Dummy task 1
@task
def dummy_task_1(prev_result: str) -> str:
    """Simple placeholder task."""
    print(f"Executing dummy_task_1 after {prev_result}")
    return "dummy_task_1_complete"

# Branch function - determines which path to take
@task
def branch_func() -> str:
    """
    Determines the next task to execute.
    In the original pipeline, this always returns 'dummy_task_3'.
    """
    # Always select dummy_task_3 as per pipeline description
    return "dummy_task_3"

# Dummy task 3 (for the branch)
@task
def dummy_task_3() -> str:
    """Branch path task."""
    print("Executing dummy_task_3 (branch path)")
    return "dummy_task_3_complete"

# Second Databricks notebook task
@task
def notebook_task_2(notebook_path: str, cluster_id: str) -> Dict[str, Any]:
    """Second notebook execution task."""
    print("Executing notebook_task_2")
    return run_notebook_task(notebook_path, cluster_id, "notebook_task_2")

# Dummy task 2 (runs after notebook_task_2)
@task
def dummy_task_2(prev_result: str) -> str:
    """Task that runs after notebook_task_2."""
    print(f"Executing dummy_task_2 after {prev_result}")
    return "dummy_task_2_complete"

# End task
@task
def end_task(prev_result: str) -> str:
    """Pipeline completion marker."""
    print(f"Pipeline completed after {prev_result}")
    return "completed"

# Main flow
@flow(name="databricks-pipeline")
def databricks_pipeline(
    notebook_path: str = "/Shared/helloworld",
    cluster_id: str = "existing-cluster-id",
):
    """
    Prefect 2.x flow that orchestrates Databricks notebook execution
    with conditional branching and parallel task execution.
    
    Args:
        notebook_path: Path to the Databricks notebook
        cluster_id: ID of the existing Databricks cluster to use
    """
    # Start pipeline
    start_result = start_task()
    
    # Execute first notebook task
    notebook_result_1 = run_notebook_task(
        notebook_path=notebook_path,
        cluster_id=cluster_id,
        task_name="notebook_task"
    )
    
    # Execute dummy task 1
    dummy1_result = dummy_task_1(notebook_result_1)
    
    # Branch decision
    branch_decision = branch_func()
    
    # Based on branch decision, execute tasks
    # Since branch always selects dummy_task_3, we can directly proceed
    # but we'll keep the structure for demonstration
    
    # Execute dummy_task_3 and notebook_task_2 in parallel
    # Submit both tasks concurrently
    dummy3_future = dummy_task_3.submit()
    notebook2_future = notebook_task_2.submit(
        notebook_path=notebook_path,
        cluster_id=cluster_id
    )
    
    # Wait for notebook_task_2 to complete before dummy_task_2
    notebook2_result = notebook2_future.result()
    dummy2_result = dummy_task_2(notebook2_result)
    
    # Wait for dummy_task_3 to complete (if needed for end_task)
    # In this case, end_task only depends on dummy_task_2, so we don't need to wait
    # But for completeness, we could wait
    dummy3_result = dummy3_future.result()
    
    # End pipeline
    end_result = end_task(dummy2_result)
    
    return end_result

if __name__ == "__main__":
    # For local execution
    databricks_pipeline(
        notebook_path="/Shared/helloworld",
        cluster_id="your-existing-cluster-id"
    )