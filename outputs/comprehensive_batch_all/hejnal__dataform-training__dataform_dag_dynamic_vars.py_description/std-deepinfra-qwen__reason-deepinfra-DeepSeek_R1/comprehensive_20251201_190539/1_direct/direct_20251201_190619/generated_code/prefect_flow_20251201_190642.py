from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import datetime
from typing import Any, Dict

# Dummy task to initiate the pipeline
@task
def start_pipeline():
    """Dummy task to start the pipeline."""
    return "Pipeline started"

# Task to parse input parameters
@task(cache_key_fn=task_input_hash)
def parse_input_parameters(logical_date: datetime, description: str) -> Dict[str, Any]:
    """Extract and process runtime parameters and store compilation configuration."""
    parameters = {
        "logical_date": logical_date,
        "description": description,
        "git_commitish": "main",  # Default git commit or branch
    }
    return parameters

# Task to create compilation result
@task
def create_compilation_result(parameters: Dict[str, Any]) -> str:
    """Retrieve configuration and execute Dataform compilation."""
    # Simulate Dataform compilation
    compilation_result_id = "compilation_result_123"
    return compilation_result_id

# Task to create workflow invocation
@task
def create_workflow_invocation(compilation_result_id: str, parameters: Dict[str, Any]) -> str:
    """Trigger Dataform workflow execution asynchronously."""
    # Simulate workflow invocation
    workflow_invocation_id = "workflow_invocation_456"
    return workflow_invocation_id

# Task to monitor workflow invocation state
@task
def is_workflow_invocation_done(workflow_invocation_id: str) -> bool:
    """Monitor the workflow invocation until it reaches SUCCEEDED or FAILED state."""
    # Simulate state monitoring
    return True

# Dummy task to mark pipeline completion
@task
def end_pipeline():
    """Dummy task to mark the pipeline completion."""
    return "Pipeline completed"

@flow(name="Dataform Transformation Pipeline")
def dataform_transformation_pipeline(logical_date: datetime, description: str):
    """Orchestrates the Dataform transformation pipeline."""
    start = start_pipeline()
    parameters = parse_input_parameters(logical_date, description)
    compilation_result_id = create_compilation_result(parameters)
    workflow_invocation_id = create_workflow_invocation(compilation_result_id, parameters)
    workflow_done = is_workflow_invocation_done(workflow_invocation_id)
    end = end_pipeline()

if __name__ == "__main__":
    # Example invocation with default parameters
    dataform_transformation_pipeline(logical_date=datetime.now(), description="Example run")