from prefect import flow, task
from prefect.context import get_run_context
from typing import Dict, Any, Optional
import os
import time
from google.cloud import dataform_v1beta1
from google.cloud.dataform_v1beta1.types import (
    CompilationResult,
    WorkflowInvocation,
    CreateCompilationResultRequest,
    CreateWorkflowInvocationRequest,
    GetWorkflowInvocationRequest,
)

# Configuration constants
DEFAULT_REGION = "us-central1"
DEFAULT_REPOSITORY = "dataform-repo"
POLL_INTERVAL_SECONDS = 30
TIMEOUT_SECONDS = 3600


@task(retries=0)
def start_pipeline() -> Dict[str, str]:
    """Initialize pipeline execution."""
    print("Pipeline execution started")
    return {"status": "started"}


@task(retries=0)
def parse_input_parameters(params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Parse runtime parameters and prepare compilation configuration.
    
    Args:
        params: Dictionary containing runtime parameters
        
    Returns:
        Compilation configuration dictionary
    """
    if params is None:
        params = {}
    
    # Extract logical date from Prefect context or parameters
    try:
        context = get_run_context()
        logical_date = context.flow_run.expected_start_time
    except Exception:
        logical_date = params.get("logical_date")
    
    config = {
        "logical_date": logical_date,
        "description": params.get("description", "Dataform compilation"),
        "git_commitish": params.get("git_commitish", "main"),
        "params": params
    }
    
    print(f"Parsed compilation config: {config}")
    return config


@task(retries=0)
def create_compilation_result(
    config: Dict[str, Any],
    project_id: str,
    region: str,
    repository_id: str
) -> str:
    """
    Create Dataform compilation result using parsed parameters.
    
    Args:
        config: Compilation configuration
        project_id: GCP project ID
        region: GCP region
        repository_id: Dataform repository ID
        
    Returns:
        Compilation result name
    """
    client = dataform_v1beta1.DataformClient()
    
    parent = f"projects/{project_id}/locations/{region}/repositories/{repository_id}"
    
    compilation_result = CompilationResult(
        git_commitish=config["git_commitish"],
        code_compilation_config=CompilationResult.CodeCompilationConfig(
            default_database=project_id
        )
    )
    
    request = CreateCompilationResultRequest(
        parent=parent,
        compilation_result=compilation_result
    )
    
    result = client.create_compilation_result(request=request)
    print(f"Created compilation result: {result.name}")
    return result.name


@task(retries=0)
def create_workflow_invocation(
    compilation_result_name: str,
    project_id: str,
    region: str
) -> str:
    """
    Create Dataform workflow invocation asynchronously.
    
    Args:
        compilation_result_name: Name of the compilation result
        project_id: GCP project ID
        region: GCP region
        
    Returns:
        Workflow invocation name
    """
    client = dataform_v1beta1.DataformClient()
    
    parent = f"projects/{project_id}/locations/{region}"
    
    workflow_invocation = WorkflowInvocation(
        compilation_result=compilation_result_name
    )
    
    request = CreateWorkflowInvocationRequest(
        parent=parent,
        workflow_invocation=workflow_invocation
    )
    
    result = client.create_workflow_invocation(request=request)
    print(f"Created workflow invocation: {result.name}")
    return result.name


@task(retries=0)
def monitor_workflow_invocation(
    invocation_name: str,
    timeout: int = TIMEOUT_SECONDS,
    poll_interval: int = POLL_INTERVAL_SECONDS
) -> str:
    """
    Monitor workflow invocation until completion.
    
    Args:
        invocation_name: Workflow invocation name
        timeout: Maximum wait time in seconds
        poll_interval: Polling interval in seconds
        
    Returns:
        Final state string
    """
    client = dataform_v1beta1.DataformClient()
    
    start_time = time.time()
    while True:
        request = GetWorkflowInvocationRequest(name=invocation_name)
        invocation = client.get_workflow_invocation(request=request)
        
        state = invocation.state
        
        if state == WorkflowInvocation.State.SUCCEEDED:
            print(f"Workflow invocation succeeded: {invocation_name}")
            return "SUCCEEDED"
        elif state in (WorkflowInvocation.State.FAILED, WorkflowInvocation.State.CANCELLED):
            print(f"Workflow invocation ended with state {state.name}: {invocation_name}")
            return state.name
        
        elapsed = time.time() - start_time
        if elapsed > timeout:
            raise TimeoutError(
                f"Workflow invocation timed out after {timeout} seconds"
            )
        
        print(
            f"Workflow invocation in progress (state: {state.name}). "
            f"Waiting {poll_interval} seconds..."
        )
        time.sleep(poll_interval)


@task(retries=0)
def end_pipeline() -> Dict[str, str]:
    """Mark pipeline completion."""
    print("Pipeline execution completed")
    return {"status": "completed"}


@flow(
    name="dataform-transformation-pipeline",
    description="Linear Dataform transformation pipeline"
)
def dataform_transformation_pipeline(
    params: Optional[Dict[str, Any]] = None,
    project_id: Optional[str] = None,
    region: str = DEFAULT_REGION,
    repository_id: str = DEFAULT_REPOSITORY
) -> Dict[str, Any]:
    """
    Orchestrate Google Cloud Dataform workflow with sequential tasks.
    
    This flow implements a linear pipeline with zero retries and immediate
    failure handling. It is triggered by dataset completion events.
    
    Args:
        params: Runtime parameters including description, git_commitish
        project_id: GCP project ID (required)
        region: GCP region for Dataform resources
        repository_id: Dataform repository ID
        
    Returns:
        Dictionary containing execution results
    """
    # Resolve project ID from parameter or environment
    if project_id is None:
        project_id = os.getenv("GCP_PROJECT_ID")
        if not project_id:
            raise ValueError(
                "project_id must be provided as parameter or GCP_PROJECT_ID "
                "environment variable"
            )
    
    # Execute tasks sequentially (linear pipeline)
    start_pipeline()
    
    config = parse_input_parameters(params)
    
    compilation_result_name = create_compilation_result(
        config=config,
        project_id=project_id,
        region=region,
        repository_id=repository_id
    )
    
    invocation_name = create_workflow_invocation(
        compilation_result_name=compilation_result_name,
        project_id=project_id,
        region=region
    )
    
    final_state = monitor_workflow_invocation(invocation_name=invocation_name)
    
    end_pipeline()
    
    return {
        "compilation_config": config,
        "compilation_result_name": compilation_result_name,
        "invocation_name": invocation_name,
        "final_state": final_state
    }


if __name__ == "__main__":
    # Local execution entry point
    # 
    # SCHEDULING NOTE: This pipeline is designed to be triggered by dataset
    # completion events. In production, deploy with an event-based trigger:
    # - Trigger: Completion of "dataform-training-data-ingestion" dataset
    # - Use Prefect Events or Cloud Run/Cloud Functions to trigger on GCS events
    #
    # Deployment example:
    # prefect deployment build dataform_pipeline.py:dataform_transformation_pipeline \
    #   -n "dataform-transformation" --tag "dataform" --apply
    #
    # Authentication: Set GOOGLE_APPLICATION_CREDENTIALS env var or use
    # Prefect GCP Credentials block
    
    # Example parameters
    example_params = {
        "description": "Training data transformation",
        "git_commitish": "main"
    }
    
    # Execute flow
    dataform_transformation_pipeline(
        params=example_params,
        project_id=os.getenv("GCP_PROJECT_ID", "your-gcp-project-id"),
        region="us-central1",
        repository_id="dataform-repo"
    )