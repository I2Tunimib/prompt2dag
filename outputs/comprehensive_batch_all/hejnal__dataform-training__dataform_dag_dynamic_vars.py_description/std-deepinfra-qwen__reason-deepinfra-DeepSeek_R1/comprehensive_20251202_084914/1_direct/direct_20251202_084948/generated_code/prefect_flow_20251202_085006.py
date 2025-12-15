from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import datetime
from typing import Any, Dict

# Dummy task to initiate the pipeline
@task
def start_pipeline():
    """Dummy task to initiate the pipeline execution."""
    return "Pipeline started"

# Task to parse input parameters
@task(cache_key_fn=task_input_hash)
def parse_input_parameters(logical_date: datetime, description: str) -> Dict[str, Any]:
    """
    Extracts and processes runtime parameters including logical date and description.
    Stores compilation configuration in XCom.
    """
    parameters = {
        "logical_date": logical_date,
        "description": description,
        "git_commitish": "main",  # Default git commit or branch
    }
    return parameters

# Task to create compilation result
@task
def create_compilation_result(parameters: Dict[str, Any]) -> str:
    """
    Retrieves the configuration from XCom and executes Dataform compilation using the parsed parameters.
    """
    # Simulate Dataform compilation
    compilation_result_id = "compilation_result_12345"
    print(f"Compilation result created: {compilation_result_id}")
    return compilation_result_id

# Task to create workflow invocation
@task
def create_workflow_invocation(compilation_result_id: str, parameters: Dict[str, Any]) -> str:
    """
    Triggers the Dataform workflow execution asynchronously using the compilation result.
    """
    # Simulate Dataform workflow invocation
    workflow_invocation_id = "workflow_invocation_67890"
    print(f"Workflow invocation created: {workflow_invocation_id}")
    return workflow_invocation_id

# Task to monitor workflow invocation state
@task
def is_workflow_invocation_done(workflow_invocation_id: str) -> bool:
    """
    Monitors the workflow invocation until it reaches either SUCCEEDED or FAILED state.
    """
    # Simulate monitoring
    print(f"Monitoring workflow invocation: {workflow_invocation_id}")
    # Simulate success
    return True

# Dummy task to mark pipeline completion
@task
def end_pipeline():
    """Dummy task to mark pipeline completion."""
    return "Pipeline completed"

@flow(name="Dataform Transformation Pipeline")
def dataform_transformation_pipeline(logical_date: datetime, description: str):
    """
    Linear data transformation pipeline for orchestrating Google Cloud Dataform workflows.
    """
    start = start_pipeline()
    parameters = parse_input_parameters(logical_date, description)
    compilation_result_id = create_compilation_result(parameters)
    workflow_invocation_id = create_workflow_invocation(compilation_result_id, parameters)
    workflow_done = is_workflow_invocation_done(workflow_invocation_id)
    end = end_pipeline()

if __name__ == "__main__":
    # Example execution with default parameters
    dataform_transformation_pipeline(datetime.now(), "Example Dataform Pipeline Run")