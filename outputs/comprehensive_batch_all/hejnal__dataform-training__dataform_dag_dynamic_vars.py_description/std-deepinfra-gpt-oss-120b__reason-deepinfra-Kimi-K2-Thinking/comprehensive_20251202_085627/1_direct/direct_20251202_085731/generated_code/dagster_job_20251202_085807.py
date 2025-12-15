from __future__ import annotations

import time
from typing import Any, Dict, Optional

from dagster import (
    Config,
    In,
    Nothing,
    Out,
    OpExecutionContext,
    op,
    job,
    get_dagster_logger,
)


class ParseParametersConfig(Config):
    """Configuration schema for parsing input parameters."""

    logical_date: Optional[str] = None
    description: Optional[str] = None
    git_commitish: Optional[str] = None


@op(out=Out(Nothing), tags={"dagster/description": "Dummy start operator."})
def start_op(_: OpExecutionContext) -> Nothing:
    """Marks the beginning of the pipeline."""
    logger = get_dagster_logger()
    logger.info("Pipeline started.")
    return Nothing


@op(
    config_schema=ParseParametersConfig,
    out=Out(Dict[str, Any]),
    tags={"dagster/description": "Parse runtime parameters and produce compilation config."},
)
def parse_parameters_op(context: OpExecutionContext) -> Dict[str, Any]:
    """Extracts runtime parameters from the run config and builds a compilation configuration."""
    cfg: ParseParametersConfig = context.op_config  # type: ignore[arg-type]
    logger = get_dagster_logger()
    logger.info("Parsing input parameters.")
    compilation_config = {
        "logical_date": cfg.logical_date or "2024-01-01",
        "description": cfg.description or "Default pipeline run",
        "git_commitish": cfg.git_commitish or "main",
    }
    logger.debug("Compilation config: %s", compilation_config)
    return compilation_config


@op(
    ins={"compilation_config": In(Dict[str, Any])},
    out=Out(Dict[str, Any]),
    tags={"dagster/description": "Simulate Dataform compilation and return a result."},
)
def create_compilation_result_op(context: OpExecutionContext, compilation_config: Dict[str, Any]) -> Dict[str, Any]:
    """Pretends to compile Dataform project using the provided configuration."""
    logger = get_dagster_logger()
    logger.info("Creating compilation result with config: %s", compilation_config)
    # In a real implementation, you would call the Dataform API here.
    # For demonstration, we generate a mock compilation identifier.
    compilation_result = {
        "compilation_id": f"comp_{int(time.time())}",
        "config_used": compilation_config,
    }
    logger.debug("Compilation result: %s", compilation_result)
    return compilation_result


@op(
    ins={"compilation_result": In(Dict[str, Any])},
    out=Out(Dict[str, Any]),
    tags={"dagster/description": "Trigger Dataform workflow invocation asynchronously."},
)
def create_workflow_invocation_op(context: OpExecutionContext, compilation_result: Dict[str, Any]) -> Dict[str, Any]:
    """Simulates triggering a Dataform workflow based on the compilation result."""
    logger = get_dagster_logger()
    logger.info("Creating workflow invocation for compilation: %s", compilation_result)
    # In a real implementation, you would invoke the Dataform workflow via its API.
    invocation = {
        "invocation_id": f"inv_{int(time.time())}",
        "compilation_id": compilation_result["compilation_id"],
    }
    logger.debug("Workflow invocation created: %s", invocation)
    return invocation


@op(
    ins={"invocation": In(Dict[str, Any])},
    out=Out(str),
    tags={"dagster/description": "Poll the workflow invocation until it reaches a terminal state."},
)
def monitor_workflow_op(context: OpExecutionContext, invocation: Dict[str, Any]) -> str:
    """Polls the workflow invocation status until it is either SUCCEEDED or FAILED."""
    logger = get_dagster_logger()
    logger.info("Monitoring workflow invocation: %s", invocation)
    # Mock polling loop – in a real scenario, replace with API calls and proper backoff.
    max_attempts = 5
    for attempt in range(1, max_attempts + 1):
        logger.debug("Polling attempt %d for invocation %s", attempt, invocation["invocation_id"])
        # Simulate a successful run on the third attempt.
        if attempt >= 3:
            status = "SUCCEEDED"
            logger.info("Workflow invocation %s completed with status: %s", invocation["invocation_id"], status)
            return status
        time.sleep(1)  # Simulate wait time between polls.
    status = "FAILED"
    logger.warning("Workflow invocation %s did not succeed after %d attempts; final status: %s", invocation["invocation_id"], max_attempts, status)
    return status


@op(
    ins={"final_status": In(str)},
    out=Out(Nothing),
    tags={"dagster/description": "Dummy end operator marking pipeline completion."},
)
def end_op(_: OpExecutionContext, final_status: str) -> Nothing:
    """Marks the end of the pipeline, logging the final workflow status."""
    logger = get_dagster_logger()
    logger.info("Pipeline completed. Final workflow status: %s", final_status)
    return Nothing


@job(
    description="Linear Dataform orchestration pipeline implemented in Dagster.",
    tags={"dagster/description": "Executes a series of steps to compile, invoke, and monitor a Dataform workflow."},
)
def dataform_pipeline_job():
    """Defines the linear execution order of the pipeline."""
    start = start_op()
    compilation_cfg = parse_parameters_op()
    compilation_res = create_compilation_result_op(compilation_cfg)
    invocation = create_workflow_invocation_op(compilation_res)
    status = monitor_workflow_op(invocation)
    end_op(status)


if __name__ == "__main__":
    # Execute the job in-process with default configuration.
    result = dataform_pipeline_job.execute_in_process(
        run_config={
            "ops": {
                "parse_parameters_op": {
                    "config": {
                        "logical_date": "2024-07-01",
                        "description": "Run from CLI",
                        "git_commitish": "feature-branch",
                    }
                }
            }
        }
    )
    if result.success:
        print("Job completed successfully.")
    else:
        print("Job failed.")