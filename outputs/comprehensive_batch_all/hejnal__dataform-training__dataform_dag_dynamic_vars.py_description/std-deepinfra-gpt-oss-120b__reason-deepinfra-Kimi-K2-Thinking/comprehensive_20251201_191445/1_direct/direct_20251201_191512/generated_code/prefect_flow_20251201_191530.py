from __future__ import annotations

import random
import time
from typing import Any, Dict, Optional

from prefect import flow, task, get_run_logger


@task
def start_task() -> None:
    """Log the start of the pipeline."""
    logger = get_run_logger()
    logger.info("Pipeline started.")


@task
def parse_input_parameters(
    logical_date: Optional[str] = None,
    description: Optional[str] = None,
    git_commitish: str = "main",
) -> Dict[str, Any]:
    """
    Extract and process runtime parameters.

    Args:
        logical_date: Logical execution date (e.g., "2024-01-01").
        description: Optional description for the run.
        git_commitish: Git reference for the Dataform code.

    Returns:
        A dictionary containing the compilation configuration.
    """
    logger = get_run_logger()
    logger.info("Parsing input parameters.")
    config = {
        "logical_date": logical_date or time.strftime("%Y-%m-%d"),
        "description": description or "Dataform pipeline execution",
        "git_commitish": git_commitish,
    }
    logger.debug("Parsed configuration: %s", config)
    return config


@task
def create_compilation_result(config: Dict[str, Any]) -> str:
    """
    Simulate Dataform compilation using the provided configuration.

    Args:
        config: Compilation configuration dictionary.

    Returns:
        A mock compilation result identifier.
    """
    logger = get_run_logger()
    logger.info("Creating compilation result with config: %s", config)
    # Placeholder for actual Dataform compilation logic.
    compilation_id = f"compilation-{random.randint(1000, 9999)}"
    logger.info("Compilation result created: %s", compilation_id)
    return compilation_id


@task
def create_workflow_invocation(compilation_id: str) -> str:
    """
    Trigger the Dataform workflow asynchronously.

    Args:
        compilation_id: Identifier of the compilation result.

    Returns:
        A mock workflow invocation identifier.
    """
    logger = get_run_logger()
    logger.info("Creating workflow invocation for compilation %s.", compilation_id)
    # Placeholder for actual Dataform workflow invocation.
    invocation_id = f"invocation-{random.randint(1000, 9999)}"
    logger.info("Workflow invocation created: %s", invocation_id)
    return invocation_id


@task
def monitor_workflow_invocation(invocation_id: str) -> str:
    """
    Poll the workflow invocation until it reaches a terminal state.

    Args:
        invocation_id: Identifier of the workflow invocation.

    Returns:
        Final state of the workflow ("SUCCEEDED" or "FAILED").
    """
    logger = get_run_logger()
    logger.info("Monitoring workflow invocation %s.", invocation_id)

    # Simulated polling loop.
    max_attempts = 10
    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        # Randomly decide if the workflow is done.
        if random.random() < 0.3:
            state = random.choice(["SUCCEEDED", "FAILED"])
            logger.info(
                "Workflow invocation %s reached terminal state: %s", invocation_id, state
            )
            return state
        logger.debug(
            "Workflow invocation %s not finished yet (attempt %d).", invocation_id, attempt
        )
        time.sleep(2)  # Simulate wait between polls.

    # If max attempts exceeded, treat as failure.
    logger.warning(
        "Workflow invocation %s did not finish after %d attempts; marking as FAILED.",
        invocation_id,
        max_attempts,
    )
    return "FAILED"


@task
def end_task(final_state: str) -> None:
    """
    Log the completion of the pipeline.

    Args:
        final_state: The final state of the workflow invocation.
    """
    logger = get_run_logger()
    logger.info("Pipeline completed with final workflow state: %s", final_state)


@flow
def dataform_pipeline_flow(
    logical_date: Optional[str] = None,
    description: Optional[str] = None,
    git_commitish: str = "main",
) -> None:
    """
    Orchestrates the Dataform transformation pipeline.

    Args:
        logical_date: Logical execution date.
        description: Description for the run.
        git_commitish: Git reference for the Dataform code.
    """
    start_task()
    config = parse_input_parameters(
        logical_date=logical_date, description=description, git_commitish=git_commitish
    )
    compilation_id = create_compilation_result(config)
    invocation_id = create_workflow_invocation(compilation_id)
    final_state = monitor_workflow_invocation(invocation_id)
    end_task(final_state)


if __name__ == "__main__":
    # Example local execution; in production, parameters would be supplied by a scheduler.
    dataform_pipeline_flow()