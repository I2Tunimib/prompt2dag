import time
import logging
from typing import Any, Dict

from prefect import flow, task, get_run_logger


@task
def start_task() -> None:
    """Dummy start task."""
    logger = get_run_logger()
    logger.info("Pipeline started.")


@task
def parse_input_parameters(
    logical_date: str,
    description: str = "",
    git_commitish: str = "main",
) -> Dict[str, Any]:
    """
    Extract and process runtime parameters.

    Returns a configuration dictionary that will be used by downstream tasks.
    """
    logger = get_run_logger()
    logger.info("Parsing input parameters.")
    config = {
        "logical_date": logical_date,
        "description": description,
        "git_commitish": git_commitish,
    }
    logger.debug("Parsed configuration: %s", config)
    return config


@task
def create_compilation_result(config: Dict[str, Any]) -> str:
    """
    Execute Dataform compilation using the parsed parameters.

    Returns a compilation result identifier.
    """
    logger = get_run_logger()
    logger.info("Creating compilation result for logical date %s.", config["logical_date"])

    # Placeholder for actual Dataform compilation logic.
    # Example using Google Cloud Dataform client:
    # from google.cloud import dataform_v1beta1 as dataform
    # client = dataform.DataformClient()
    # request = dataform.CompileWorkflowInvocationRequest(
    #     parent="projects/PROJECT/locations/LOCATION/repositories/REPO",
    #     compilation_result=...
    # )
    # response = client.compile_workflow_invocation(request=request)
    # compilation_result_id = response.name

    compilation_result_id = f"compilation-{config['logical_date']}"
    logger.debug("Compilation result ID: %s", compilation_result_id)
    return compilation_result_id


@task
def create_workflow_invocation(
    compilation_result_id: str, config: Dict[str, Any]
) -> str:
    """
    Trigger the Dataform workflow execution asynchronously.

    Returns a workflow invocation identifier.
    """
    logger = get_run_logger()
    logger.info(
        "Creating workflow invocation for compilation result %s.", compilation_result_id
    )

    # Placeholder for actual Dataform workflow invocation logic.
    # Example:
    # request = dataform.CreateWorkflowInvocationRequest(
    #     parent="projects/PROJECT/locations/LOCATION/repositories/REPO",
    #     workflow_invocation=dataform.WorkflowInvocation(
    #         compilation_result=compilation_result_id,
    #         ...
    #     )
    # )
    # response = client.create_workflow_invocation(request=request)
    # invocation_id = response.name

    invocation_id = f"invocation-{compilation_result_id}"
    logger.debug("Workflow invocation ID: %s", invocation_id)
    return invocation_id


def _mock_get_invocation_state(invocation_id: str) -> str:
    """
    Mock function to simulate checking the state of a workflow invocation.

    In a real implementation, this would query the Dataform API.
    """
    # For demonstration, we assume the workflow succeeds after a short wait.
    return "SUCCEEDED"


@task
def monitor_workflow_invocation(invocation_id: str) -> str:
    """
    Poll the workflow invocation until it reaches a terminal state.

    Returns the final state ('SUCCEEDED' or 'FAILED').
    """
    logger = get_run_logger()
    logger.info("Monitoring workflow invocation %s.", invocation_id)

    max_attempts = 30
    attempt = 0
    while attempt < max_attempts:
        state = _mock_get_invocation_state(invocation_id)
        logger.debug("Attempt %d: invocation state = %s", attempt + 1, state)
        if state in ("SUCCEEDED", "FAILED"):
            logger.info("Workflow invocation %s finished with state: %s", invocation_id, state)
            return state
        time.sleep(10)
        attempt += 1

    logger.error("Workflow invocation %s did not reach a terminal state within the timeout.", invocation_id)
    raise RuntimeError("Workflow invocation monitoring timed out.")


@task
def end_task() -> None:
    """Dummy end task."""
    logger = get_run_logger()
    logger.info("Pipeline completed.")


@flow
def dataform_transformation_flow(
    logical_date: str,
    description: str = "",
    git_commitish: str = "main",
) -> None:
    """
    Orchestrates the Dataform transformation pipeline.

    Args:
        logical_date: Logical date for the pipeline run.
        description: Optional description of the run.
        git_commitish: Git commit/branch to use for the Dataform code.
    """
    start_task()
    config = parse_input_parameters(
        logical_date=logical_date,
        description=description,
        git_commitish=git_commitish,
    )
    compilation_result_id = create_compilation_result(config)
    invocation_id = create_workflow_invocation(compilation_result_id, config)
    monitor_workflow_invocation(invocation_id)
    end_task()


if __name__ == "__main__":
    # Example local execution with default parameters.
    # In production, Prefect deployments would provide the parameters.
    dataform_transformation_flow(
        logical_date="2024-01-01",
        description="Sample run",
        git_commitish="main",
    )