from dagster import (
    op,
    job,
    Out,
    In,
    RetryPolicy,
    resource,
    Field,
    StringSource,
)
import time
from datetime import datetime
from typing import Any, Dict


@resource
def dataform_resource(context):
    """Stub resource for Google Cloud Dataform client.
    
    Production implementation would initialize google.cloud.dataform_v1beta1.DataformClient
    using credentials from the 'modelling_cloud_default' connection.
    """
    class DataformClientStub:
        def compile_workflow(self, config: Dict[str, Any]) -> str:
            context.log.info(f"Compiling Dataform workflow with config: {config}")
            return "compilation-result-id-12345"
        
        def invoke_workflow(self, compilation_result_id: str) -> str:
            context.log.info(f"Invoking Dataform workflow for compilation result: {compilation_result_id}")
            return "workflow-invocation-id-67890"
        
        def get_workflow_state(self, workflow_invocation_id: str) -> str:
            return "SUCCEEDED"
    
    return DataformClientStub()


@op(
    out=Out(bool),
    retry_policy=RetryPolicy(max_retries=0),
)
def start_op(context) -> bool:
    """Initiates the pipeline execution."""
    context.log.info("Starting Dataform transformation pipeline")
    return True


@op(
    ins={"start_signal": In(bool)},
    out=Out(Dict[str, Any]),
    config_schema={
        "logical_date": Field(StringSource, is_required=False),
        "description": Field(StringSource, is_required=False),
        "git_commitish": Field(StringSource, is_required=False, default_value="main"),
    },
    retry_policy=RetryPolicy(max_retries=0),
)
def parse_input_parameters_op(context, start_signal: bool) -> Dict[str, Any]:
    """Parse runtime parameters and create compilation configuration."""
    _ = start_signal
    
    config = context.op_config or {}
    
    compilation_config = {
        "logical_date": config.get("logical_date", datetime.utcnow().isoformat()),
        "description": config.get("description", "Dataform training pipeline"),
        "git_commitish": config.get("git_commitish", "main"),
        "project_id": "your-gcp-project",
        "region": "us-central1",
        "repository": "your-dataform-repo",
    }
    
    context.log.info(f"Parsed compilation config: {compilation_config}")
    return compilation_config


@op(
    ins={"compilation_config": In(Dict[str, Any])},
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=0),
    required_resource_keys={"dataform"},
)
def create_compilation_result_op(context, compilation_config: Dict[str, Any]) -> str:
    """Create Dataform compilation result using the parsed configuration."""
    dataform_client = context.resources.dataform
    compilation_result_id = dataform_client.compile_workflow(compilation_config)
    context.log.info(f"Created compilation result: {compilation_result_id}")
    return compilation_result_id


@op(
    ins={"compilation_result_id": In(str)},
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=0),
    required_resource_keys={"dataform"},
)
def create_workflow_invocation_op(context, compilation_result_id: str) -> str:
    """Create Dataform workflow invocation from compilation result."""
    dataform_client = context.resources.dataform
    workflow_invocation_id = dataform_client.invoke_workflow(compilation_result_id)
    context.log.info(f"Created workflow invocation: {workflow_invocation_id}")
    return workflow_invocation_id


@op(
    ins={"workflow_invocation_id": In(str)},
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=0),
    required_resource_keys={"dataform"},
)
def monitor_workflow_invocation_op(context, workflow_invocation_id: str) -> str:
    """Monitor Dataform workflow invocation until completion."""
    dataform_client = context.resources.dataform
    
    max_attempts = 60
    attempt = 0
    
    while attempt < max_attempts:
        state = dataform_client.get_workflow_state(workflow_invocation_id)
        context.log.info(f"Workflow {workflow_invocation_id} state: {state}")
        
        if state == "SUCCEEDED":
            context.log.info("Workflow completed successfully")
            return state
        elif state == "FAILED":
            raise Exception(f"Dataform workflow {workflow_invocation_id} failed")
        
        attempt += 1
        time.sleep(30)
    
    raise Exception(f"Workflow {workflow_invocation_id} timed out after {max_attempts} attempts")


@op(
    ins={"workflow_state": In(str)},
    out=Out(bool),
    retry_policy=RetryPolicy(max_retries=0),
)
def end_op(context, workflow_state: str) -> bool:
    """Marks pipeline completion."""
    _ = workflow_state
    context.log.info("Dataform transformation pipeline completed successfully")
    return True


@job(
    resource_defs={
        "dataform": dataform_resource,
    },
)
def dataform_transformation_pipeline():
    """Linear data transformation pipeline orchestrating Google Cloud Dataform workflows."""
    start = start_op()
    config = parse_input_parameters_op(start)
    compilation_result = create_compilation_result_op(config)
    workflow_invocation = create_workflow_invocation_op(compilation_result)
    workflow_state = monitor_workflow_invocation_op(workflow_invocation)
    end_op(workflow_state)


if __name__ == "__main__":
    result = dataform_transformation_pipeline.execute_in_process(
        run_config={
            "ops": {
                "parse_input_parameters_op": {
                    "config": {
                        "logical_date": "2023-01-01T00:00:00",
                        "description": "Training data transformation",
                        "git_commitish": "main",
                    }
                }
            }
        }
    )