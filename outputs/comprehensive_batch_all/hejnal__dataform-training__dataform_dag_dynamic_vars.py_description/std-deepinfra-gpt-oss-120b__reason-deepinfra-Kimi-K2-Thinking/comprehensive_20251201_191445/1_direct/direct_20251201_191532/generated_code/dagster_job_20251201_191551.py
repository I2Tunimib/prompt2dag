from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict

from dagster import (
    Config,
    OpExecutionContext,
    ResourceDefinition,
    asset,
    job,
    op,
    resource,
)


@dataclass
class DataformCompilationResult:
    """Result of a Dataform compilation."""

    compiled_sql: str
    metadata: Dict[str, Any]


@dataclass
class DataformInvocationResult:
    """Result of a Dataform workflow invocation."""

    invocation_id: str


class DataformResource:
    """A minimal stub for interacting with Google Cloud Dataform."""

    def compile(self, parameters: Dict[str, Any]) -> DataformCompilationResult:
        """Simulate compilation using provided parameters."""
        compiled_sql = f"-- Compiled with params: {parameters}"
        metadata = {"timestamp": time.time()}
        return DataformCompilationResult(compiled_sql=compiled_sql, metadata=metadata)

    def invoke_workflow(self, compilation: DataformCompilationResult) -> DataformInvocationResult:
        """Simulate asynchronous workflow invocation."""
        invocation_id = f"invocation-{int(time.time())}"
        return DataformInvocationResult(invocation_id=invocation_id)

    def check_invocation_state(self, invocation_id: str) -> str:
        """Simulate polling the workflow state."""
        # In a real implementation this would query the Dataform API.
        # Here we simply wait a short period and return a succeeded state.
        time.sleep(1)
        return "SUCCEEDED"


@resource
def dataform_resource() -> DataformResource:
    """Dagster resource providing a Dataform client stub."""
    return DataformResource()


class ParseParamsConfig(Config):
    """Configuration schema for the parse_params op."""

    logical_date: str = "2024-01-01"
    description: str = "Default run description"
    git_commitish: str = "main"


@op(required_resource_keys={"dataform"})
def start(context: OpExecutionContext) -> None:
    """Dummy start operation."""
    context.log.info("Pipeline started.")


@op(config_schema=ParseParamsConfig, required_resource_keys={"dataform"})
def parse_params(context: OpExecutionContext) -> Dict[str, Any]:
    """Extract and process runtime parameters."""
    cfg: ParseParamsConfig = context.op_config
    params = {
        "logical_date": cfg.logical_date,
        "description": cfg.description,
        "git_commitish": cfg.git_commitish,
    }
    context.log.info(f"Parsed parameters: {params}")
    return params


@op(required_resource_keys={"dataform"})
def compile_dataform(context: OpExecutionContext, params: Dict[str, Any]) -> DataformCompilationResult:
    """Execute Dataform compilation using parsed parameters."""
    dataform: DataformResource = context.resources.dataform
    result = dataform.compile(params)
    context.log.info(f"Compilation result: {result}")
    return result


@op(required_resource_keys={"dataform"})
def invoke_workflow(
    context: OpExecutionContext, compilation: DataformCompilationResult
) -> DataformInvocationResult:
    """Trigger the Dataform workflow asynchronously."""
    dataform: DataformResource = context.resources.dataform
    invocation = dataform.invoke_workflow(compilation)
    context.log.info(f"Workflow invoked with ID: {invocation.invocation_id}")
    return invocation


@op(required_resource_keys={"dataform"})
def monitor_workflow(
    context: OpExecutionContext, invocation: DataformInvocationResult
) -> str:
    """Poll the workflow invocation until it reaches a terminal state."""
    dataform: DataformResource = context.resources.dataform
    state = dataform.check_invocation_state(invocation.invocation_id)
    context.log.info(f"Workflow {invocation.invocation_id} finished with state: {state}")
    return state


@op(required_resource_keys={"dataform"})
def end(context: OpExecutionContext, final_state: str) -> None:
    """Dummy end operation marking pipeline completion."""
    if final_state != "SUCCEEDED":
        raise RuntimeError(f"Workflow finished with unexpected state: {final_state}")
    context.log.info("Pipeline completed successfully.")


@job(
    resource_defs={"dataform": dataform_resource},
    description="Linear Dataform transformation pipeline.",
)
def dataform_pipeline():
    """Dagster job orchestrating the Dataform workflow."""
    start()
    params = parse_params()
    compilation = compile_dataform(params)
    invocation = invoke_workflow(compilation)
    final_state = monitor_workflow(invocation)
    end(final_state)


if __name__ == "__main__":
    result = dataform_pipeline.execute_in_process(
        run_config={
            "ops": {
                "parse_params": {
                    "config": {
                        "logical_date": "2024-07-01",
                        "description": "Run from CLI",
                        "git_commitish": "feature-branch",
                    }
                }
            }
        }
    )
    assert result.success