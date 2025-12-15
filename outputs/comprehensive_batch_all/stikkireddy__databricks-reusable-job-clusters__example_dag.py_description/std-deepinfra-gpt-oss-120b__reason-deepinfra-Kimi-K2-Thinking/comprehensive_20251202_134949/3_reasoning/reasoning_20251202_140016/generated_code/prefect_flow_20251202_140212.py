from prefect import flow, task
from prefect_databricks import DatabricksCredentials
from prefect_databricks.jobs import jobs_submit_run_and_wait_for_completion
import time

# Dummy tasks
@task
def start_task():
    """Entry point task"""
    print("Starting pipeline")
    return "started"

@task
def dummy_task_1():
    """Intermediate dummy task"""
    print("Executing dummy_task_1")
    return "dummy_1_complete"

@task
def branch_func():
    """
    Branching logic - always returns dummy_task_3 path
    In Airflow this would return a task_id, in Prefect we can return a value
    """
    print("Evaluating branch condition")
    # Always select dummy_task_3 path
    return "dummy_task_3"

@task
def dummy_task_3():
    """Task that runs in parallel with notebook_task_2"""
    print("Executing dummy_task_3")
    return "dummy_3_complete"

@task
def dummy_task_2():
    """Task that runs after notebook_task_2"""
    print("Executing dummy_task_2")
    return "dummy_2_complete"

@task
def end_task():
    """Final task marking pipeline completion"""
    print("Pipeline completed")
    return "completed"

# Databricks tasks
@task
def notebook_task():
    """
    Execute helloworld notebook on existing Databricks cluster
    """
    # In a real scenario, you'd load credentials from a block
    # credentials = DatabricksCredentials.load("databricks-default")
    
    # For this example, we'll simulate the execution
    print("Executing notebook_task: helloworld notebook")
    # Simulate notebook execution
    time.sleep(2)
    return "notebook_task_complete"

@task
def notebook_task_2():
    """
    Execute helloworld notebook on existing Databricks cluster
    """
    print("Executing notebook_task_2: helloworld notebook")
    # Simulate notebook execution
    time.sleep(2)
    return "notebook_task_2_complete"

@flow
def databricks_pipeline_flow():
    """
    Prefect 2.x flow that orchestrates Databricks notebook execution
    with conditional branching and parallel execution
    """
    # Start the pipeline
    start = start_task()
    
    # Execute first Databricks notebook
    notebook1 = notebook_task(wait_for=[start])
    
    # Proceed to dummy_task_1
    dummy1 = dummy_task_1(wait_for=[notebook1])
    
    # Branching logic - evaluate condition
    branch_result = branch_func(wait_for=[dummy1])
    
    # Based on branch logic, we always go to dummy_task_3
    # In Prefect, we can conditionally execute tasks based on the result
    # Since it always returns "dummy_task_3", we execute dummy_task_3
    # and notebook_task_2 in parallel
    
    # Execute dummy_task_3 and notebook_task_2 in parallel
    # Use .submit() for concurrent execution
    dummy3_future = dummy_task_3.submit(wait_for=[branch_result])
    notebook2_future = notebook_task_2.submit(wait_for=[branch_result])
    
    # dummy_task_2 runs after notebook_task_2 completes
    dummy2 = dummy_task_2(wait_for=[notebook2_future])
    
    # end_task runs after both parallel paths complete
    # Wait for both dummy_task_3 and dummy_task_2
    end = end_task(wait_for=[dummy3_future, dummy2])
    
    return end

if __name__ == '__main__':
    # Manual trigger only - no schedule
    databricks_pipeline_flow()