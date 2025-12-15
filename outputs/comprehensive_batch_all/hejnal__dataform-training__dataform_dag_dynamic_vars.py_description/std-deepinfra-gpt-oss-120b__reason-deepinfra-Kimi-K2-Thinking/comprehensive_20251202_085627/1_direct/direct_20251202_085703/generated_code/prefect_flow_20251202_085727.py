import logging
import time
from datetime import datetime
from typing import Any, Dict, Optional

from prefect import flow, task

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@task
def start_task() -> None:
    """Dummy start task to initiate the pipeline."""
    logger.info("Pipeline started.")


@task
def parse_input_parameters(run_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Extract and process runtime parameters.

    Args:
        run_config: Optional dictionary of runtime parameters (e.g., from Prefect context).

    Returns:
        A dictionary containing the compilation configuration.
    """
    config = run_config or {}
    logical_date = config.get("logical_date", datetime.utcnow().isoformat())
    description = config.get("description", "Dataform compilation run")
    git_commitish = config.get("git_commitish", "main")
    compilation_config = {
        "logical_date": logical_date,
        "description": description,
        "git_commitish": git_commitish,
    }
    logger.info("Parsed input parameters: %s", compilation_config)
    return compilation_config


@task
def create_compilation_result(compilation_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute Dataform compilation using the provided configuration.

    This function is a placeholder; replace with actual Dataform client calls.

    Args:
        compilation_config: Configuration dict from ``parse_input_parameters``.

    Returns:
        A dictionary representing the compilation result (e.g., result ID).
    """
    # Placeholder for real compilation logic.
    # Example using google-cloud-dataform:
    # client = dataform_v1beta1.DataformClient()
    # request = dataform_v1beta1.CompileWorkflowInvocationRequest(...)
    # response = client.compile_workflow_invocation(request=request)
    # return {"compilation_result_id": response.name}
    compilation_result = {
        "compilation_result_id": f"comp-{int(time.time())}",
        "config_used": compilation_config,
    }
    logger.info("Created compilation result: %s", compilation_result)
    return compilation_result


@task
def create_workflow_invocation(compilation_result: Dict[str, Any]) -> str:
    """
    Trigger the Dataform workflow execution asynchronously.

    This function is a placeholder; replace with actual Dataform invocation logic.

    Args:
        compilation_result: Result dict from ``create_compilation_result``.

    Returns:
        The identifier of the created workflow invocation.
    """
    # Placeholder for real invocation logic.
    # Example:
    # client = dataform_v1beta1.DataformClient()
    # request = dataform_v1beta1.CreateWorkflowInvocationRequest(...)
    # response = client.create_workflow_invocation(request=request)
    # return response.name
    invocation_id = f"invocation-{int(time.time())}"
    logger.info(
        "Created workflow invocation %s using compilation result %s",
        invocation_id,
        compilation_result["compilation_result_id"],
    )
    return invocation_id


@task
def monitor_workflow_invocation(invocation_id: str, poll_interval: int = 10) -> str:
    """
    Poll the workflow invocation until it reaches a terminal state.

    This function is a placeholder; replace with actual Dataform status checks.

    Args:
        invocation_id: Identifier of the workflow invocation to monitor.
        poll_interval: Seconds between status checks.

    Returns:
        Final state of the workflow invocation (e.g., "SUCCEEDED" or "FAILED").
    """
    # Placeholder loop simulating status checks.
    # Replace with real API call to fetch invocation state.
    terminal_states = {"SUCCEEDED", "FAILED"}
    current_state = "RUNNING"
    elapsed = 0
    logger.info("Monitoring workflow invocation %s", invocation_id)

    while current_state not in terminal_states:
        time.sleep(poll_interval)
        elapsed += poll_interval
        # Simulated state transition after 30 seconds.
        if elapsed >= 30:
            current_state = "SUCCEEDED"
        logger.debug(
            "Invocation %s current state: %s (elapsed %ds)",
            invocation_id,
            current_state,
            elapsed,
        )

    logger.info(
        "Workflow invocation %s reached terminal state: %s", invocation_id, current_state
    )
    return current_state


@task
def end_task(final_state: str) -> None:
    """Dummy end task to mark pipeline completion."""
    logger.info("Pipeline completed with final workflow state: %s", final_state)


@flow
def dataform_transformation_flow(run_config: Optional[Dict[str, Any]] = None) -> None:
    """
    Orchestrates the Dataform transformation pipeline.

    The flow executes six sequential steps:
    1. Start dummy task
    2. Parse input parameters
    3. Create compilation result
    4. Create workflow invocation
    5. Monitor workflow invocation until completion
    6. End dummy task
    """
    start_task()
    compilation_config = parse_input_parameters(run_config)
    compilation_result = create_compilation_result(compilation_config)
    invocation_id = create_workflow_invocation(compilation_result)
    final_state = monitor_workflow_invocation(invocation_id)
    end_task(final_state)


if __name__ == "__main__":
    # Example invocation; replace with actual runtime configuration as needed.
    example_config = {
        "logical_date": "2024-01-01T00:00:00Z",
        "description": "Run from Prefect flow",
        "git_commitish": "main",
    }
    dataform_transformation_flow(run_config=example_config)