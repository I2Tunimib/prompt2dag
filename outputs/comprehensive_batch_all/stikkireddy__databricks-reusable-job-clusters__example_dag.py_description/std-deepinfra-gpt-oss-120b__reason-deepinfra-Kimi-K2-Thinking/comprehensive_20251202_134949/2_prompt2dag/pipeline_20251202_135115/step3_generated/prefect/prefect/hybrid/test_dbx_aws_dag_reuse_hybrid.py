from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name='start_pipeline', retries=0)
def start_pipeline():
    """Task: Start Pipeline"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='execute_primary_notebook', retries=1)
def execute_primary_notebook():
    """Task: Execute Primary Databricks Notebook"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='intermediate_dummy_1', retries=0)
def intermediate_dummy_1():
    """Task: Intermediate Dummy 1"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='determine_branch_path', retries=0)
def determine_branch_path():
    """Task: Determine Branch Path"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='execute_secondary_notebook', retries=1)
def execute_secondary_notebook():
    """Task: Execute Secondary Databricks Notebook"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='terminal_branch_dummy', retries=0)
def terminal_branch_dummy():
    """Task: Terminal Branch Dummy"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='intermediate_dummy_2', retries=0)
def intermediate_dummy_2():
    """Task: Intermediate Dummy 2"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='pipeline_completion', retries=0)
def pipeline_completion():
    """Task: Pipeline Completion"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(name='test_dbx_aws_dag_reuse', task_runner=SequentialTaskRunner())
def test_dbx_aws_dag_reuse():
    # Entry point
    start = start_pipeline()

    # Primary notebook execution
    primary = execute_primary_notebook(wait_for=[start])

    # Intermediate step after primary notebook
    dummy1 = intermediate_dummy_1(wait_for=[primary])

    # Determine branching path
    branch_decision = determine_branch_path(wait_for=[dummy1])

    # Fan‑out: two parallel branches (handled sequentially by the runner)
    terminal_dummy = terminal_branch_dummy(wait_for=[branch_decision])
    secondary = execute_secondary_notebook(wait_for=[branch_decision])

    # Continue downstream from the secondary branch
    dummy2 = intermediate_dummy_2(wait_for=[secondary])

    # Final completion task
    pipeline_completion(wait_for=[dummy2, terminal_dummy])


if __name__ == '__main__':
    test_dbx_aws_dag_reuse()