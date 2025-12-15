from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name="initialize_pipeline", retries=0)
def initialize_pipeline():
    """Task: Initialize Pipeline"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="parse_input_params", retries=0)
def parse_input_params():
    """Task: Parse Input Parameters"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="create_compilation_result", retries=0)
def create_compilation_result():
    """Task: Create Dataform Compilation Result"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="create_workflow_invocation", retries=0)
def create_workflow_invocation():
    """Task: Create Dataform Workflow Invocation"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="monitor_workflow_state", retries=0)
def monitor_workflow_state():
    """Task: Monitor Workflow Invocation State"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="finalize_pipeline", retries=0)
def finalize_pipeline():
    """Task: Finalize Pipeline"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(
    name="data_transformation_pipeline",
    task_runner=SequentialTaskRunner(),
)
def data_transformation_pipeline():
    """
    Sequential pipeline orchestrating data transformation steps.
    """
    initialize_pipeline()
    parse_input_params()
    create_compilation_result()
    create_workflow_invocation()
    monitor_workflow_state()
    finalize_pipeline()


if __name__ == "__main__":
    data_transformation_pipeline()