from __future__ import annotations

import time
from typing import Any, Dict

from dagster import (
    Config,
    In,
    Nothing,
    OpExecutionContext,
    Out,
    ResourceDefinition,
    job,
    op,
)


class DataformResource:
    """A minimal stub for interacting with Google Cloud Dataform.

    In a real deployment this would wrap the Dataform client library and use the
    ``modelling_cloud_default`` connection.
    """

    def compile(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Simulate a Dataform compilation.

        Returns a dictionary containing a compilation identifier.
        """
        compilation_id = f"compilation-{int(time.time())}"
        return {"compilation_id": compilation_id, "parameters": parameters}

    def create_workflow_invocation(self, compilation_result: Dict[str, Any]) -> str:
        """Simulate creating a workflow invocation.

        Returns a workflow invocation identifier.
        """
        invocation_id = f"invocation-{int(time.time())}"
        return invocation_id

    def get_workflow_state(self, invocation_id: str) -> str:
        """Simulate polling the workflow state.

        Returns either ``SUCCEEDED`` or ``FAILED`` after a short delay.
        """
        # In a real implementation this would query the Dataform API.
        # Here we simply wait and then return a successful state.
        time.sleep(1)
        return "SUCCEEDED"


def dataform_resource() -> ResourceDefinition:
    """Factory for the DataformResource."""
    return ResourceDefinition.hardcoded_resource(DataformResource())


class ParseParametersConfig(Config):
    """Configuration schema for the ``parse_parameters`` op."""

    logical_date: str = "2024-01-01"
    description: str = "Default run description"
    git_commitish: str = "main"
    other_params: Dict[str, Any] = {}


@op(out=Out(dict), required_resource_keys={"dataform"})
def start_op(_: OpExecutionContext) -> Nothing:
    """Dummy start operation to mark the beginning of the pipeline."""
    return None


@op(
    out=Out(dict),
    config_schema=ParseParametersConfig,
    required_resource_keys={"dataform"},
)
def parse_parameters_op(context: OpExecutionContext) -> Dict[str, Any]:
    """Extract and process runtime parameters.

    The parameters are supplied via the job run config and are returned as a
    dictionary that downstream ops can consume.
    """
    cfg: ParseParametersConfig = context.op_config  # type: ignore
    parameters = {
        "logical_date": cfg.logical_date,
        "description": cfg.description,
        "git_commitish": cfg.git_commitish,
        **cfg.other_params,
    }
    context.log.info("Parsed parameters: %s", parameters)
    return parameters


@op(
    out=Out(dict),
    ins={"parameters": In(dict)},
    required_resource_keys={"dataform"},
)
def create_compilation_result_op(
    context: OpExecutionContext, parameters: Dict[str, Any]
) -> Dict[str, Any]:
    """Execute Dataform compilation using the parsed parameters."""
    dataform: DataformResource = context.resources.dataform
    compilation_result = dataform.compile(parameters)
    context.log.info("Compilation result: %s", compilation_result)
    return compilation_result


@op(
    out=Out(str),
    ins={"compilation_result": In(dict)},
    required_resource_keys={"dataform"},
)
def create_workflow_invocation_op(
    context: OpExecutionContext, compilation_result: Dict[str, Any]
) -> str:
    """Trigger the Dataform workflow execution asynchronously."""
    dataform: DataformResource = context.resources.dataform
    invocation_id = dataform.create_workflow_invocation(compilation_result)
    context.log.info("Created workflow invocation: %s", invocation_id)
    return invocation_id


@op(
    out=Out(str),
    ins={"invocation_id": In(str)},
    required_resource_keys={"dataform"},
)
def monitor_workflow_op(
    context: OpExecutionContext, invocation_id: str
) -> str:
    """Poll the workflow invocation until it reaches a terminal state."""
    dataform: DataformResource = context.resources.dataform
    max_attempts = 10
    attempt = 0
    while attempt < max_attempts:
        state = dataform.get_workflow_state(invocation_id)
        context.log.info(
            "Polling invocation %s: attempt %d, state %s",
            invocation_id,
            attempt + 1,
            state,
        )
        if state in {"SUCCEEDED", "FAILED"}:
            break
        time.sleep(2)
        attempt += 1
    else:
        state = "UNKNOWN"
        context.log.warning(
            "Workflow invocation %s did not reach a terminal state after %d attempts",
            invocation_id,
            max_attempts,
        )
    return state


@op(ins={"final_state": In(str)})
def end_op(_: OpExecutionContext, final_state: str) -> Nothing:
    """Dummy end operation to mark pipeline completion."""
    # In a real pipeline you might perform cleanup or notifications here.
    return None


@job(resource_defs={"dataform": dataform_resource()})
def dataform_pipeline_job():
    """Linear Dagster job orchestrating a Dataform workflow."""
    start = start_op()
    parameters = parse_parameters_op()
    compilation = create_compilation_result_op(parameters)
    invocation = create_workflow_invocation_op(compilation)
    final_state = monitor_workflow_op(invocation)
    end_op(final_state)


if __name__ == "__main__":
    result = dataform_pipeline_job.execute_in_process(
        run_config={
            "ops": {
                "parse_parameters_op": {
                    "config": {
                        "logical_date": "2024-07-01",
                        "description": "Run triggered by ingestion completion",
                        "git_commitish": "main",
                        "other_params": {"example_key": "example_value"},
                    }
                }
            }
        }
    )
    assert result.success