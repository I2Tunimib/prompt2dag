import time
from datetime import datetime
from typing import Dict, Any, Optional

from prefect import flow, task, get_run_logger
from prefect.context import get_run_context
from google.cloud import dataform_v1beta1
from prefect_gcp.credentials import GcpCredentials


@task(retries=0)
def start_task() -> Dict[str, str]:
    """Initiate pipeline execution."""
    logger = get_run_logger()
    logger.info("Starting Dataform pipeline execution")
    return {"status": "started"}


@task(retries=0)
def parse_input_parameters(
    logical_date: Optional[str] = None,
    description: Optional[str] = None,
    git_commitish: str = "main"
) -> Dict[str, Any]:
    """Extract and process runtime parameters."""
    logger = get_run_logger()
    
    if logical_date is None:
        try:
            context = get_run_context()
            logical_date = context.flow_run.expected_start_time.isoformat()
        except Exception:
            logical_date = datetime.utcnow().isoformat()
    
    if description is None:
        description = "Dataform workflow execution"
    
    config = {
        "logical_date": logical_date,
        "description": description,
        "git_commitish": git_commitish,
    }
    
    logger.info(f"Parsed parameters: {config}")
    return config


@task(retries=0)
def create_compilation_result(
    config: Dict[str, Any],
    gcp_credentials: GcpCredentials,
    project_id: str,
    region: str,
    repository_id: str
) -> str:
    """Execute Dataform compilation using parsed parameters."""
    logger = get_run_logger()
    
    credentials = gcp_credentials.get_credentials()
    client = dataform_v1beta1.DataformClient(credentials=credentials)
    
    parent = f"projects/{project_id}/locations/{region}/repositories/{repository_id}"
    
    compilation_result = dataform_v1beta1.CompilationResult(
        git_commitish=config["git_commitish"],
        code_compilation_config=dataform_v1beta1.CodeCompilationConfig(
            default_database=project_id,
            default_schema="dataform",
        )
    )
    
    response = client.create_compilation_result(
        parent=parent,
        compilation_result=compilation_result
    )
    
    compilation_result_id = response.name.split("/")[-1]
    logger.info(f"Created compilation result: {compilation_result_id}")
    
    return compilation_result_id


@task(retries=0)
def create_workflow_invocation(
    compilation_result_id: str,
    config: Dict[str, Any],
    gcp_credentials: GcpCredentials,
    project_id: str,
    region: str,
    repository_id: str
) -> str:
    """Trigger Dataform workflow execution asynchronously."""
    logger = get_run_logger()
    
    credentials = gcp_credentials.get_credentials()
    client = dataform_v1beta1.DataformClient(credentials=credentials)
    
    parent = f"projects/{project_id}/locations/{region}/repositories/{repository_id}"
    
    workflow_invocation = dataform_v1beta1.WorkflowInvocation(
        compilation_result=f"{parent}/compilationResults/{compilation_result_id}",
        invocation_config=dataform_v1beta1.InvocationConfig(
            included_tags=["prod"],
        )
    )
    
    response = client.create_workflow_invocation(
        parent=parent,
        workflow_invocation=workflow_invocation
    )
    
    workflow_invocation_id = response.name.split("/")[-1]
    logger.info(f"Created workflow invocation: {workflow_invocation_id}")
    
    return workflow_invocation_id


@task(retries=0)
def monitor_workflow_invocation(
    workflow_invocation_id: str,
    gcp_credentials: GcpCredentials,
    project_id: str,
    region: str,
    repository_id: str,
    poll_interval: int = 30,
    timeout: int = 3600
) -> str:
    """Monitor workflow invocation until completion."""
    logger = get_run_logger()
    
    credentials = gcp_credentials.get_credentials()
    client = dataform_v1beta1.DataformClient(credentials=credentials)
    
    name = (
        f"projects/{project_id}/locations/{region}/repositories/{repository_id}"
        f"/workflowInvocations/{workflow_invocation_id}"
    )
    
    start_time = time.time()
    
    while True:
        response = client.get_workflow_invocation(name=name)
        state = response.state.name
        
        logger.info(f"Workflow invocation state: {state}")
        
        if state == "SUCCEEDED":
            logger.info("Workflow invocation succeeded")
            return "SUCCEEDED"
        elif state in ["FAILED", "CANCELLED"]:
            logger.error(f"Workflow invocation {state}")
            raise Exception(f"Workflow invocation {workflow_invocation_id} {state}")
        
        if time.time() - start_time > timeout:
            raise Exception(
                f"Timeout waiting for workflow invocation {workflow_invocation_id}"
            )
        
        time.sleep(poll_interval)


@task(retries=0)
def end_task() -> Dict[str, str]:
    """Mark pipeline completion."""
    logger = get_run_logger()
    logger.info("Dataform pipeline execution completed")
    return {"status": "completed"}


@flow(name="dataform-transformation-pipeline")
def dataform_transformation_pipeline(
    project_id: str = "your-gcp-project",
    region: str = "us-central1",
    repository_id: str = "your-dataform-repo",
    git_commitish: str = "main",
    description: Optional[str] = None,
    logical_date: Optional[str] = None
) -> Dict[str, Any]:
    """
    Linear data transformation pipeline orchestrating Google Cloud Dataform workflows.
    
    Args:
        project_id: GCP project ID
        region: GCP region where Dataform repository is located
        repository_id: Dataform repository ID
        git_commitish: Git commit/branch/tag to use (default: "main")
        description: Optional description for the workflow
        logical_date: Optional logical date for the run
    """
    
    gcp_credentials = GcpCredentials.load("modelling_cloud_default")
    
    # Execute tasks sequentially (linear pipeline)
    start_result = start_task()
    
    config = parse_input_parameters(
        logical_date=logical_date,
        description=description,
        git_commitish=git_commitish
    )
    
    compilation_result_id = create_compilation_result(
        config=config,
        gcp_credentials=gcp_credentials,
        project_id=project_id,
        region=region,
        repository_id=repository_id
    )
    
    workflow_invocation_id = create_workflow_invocation(
        compilation_result_id=compilation_result_id,
        config=config,
        gcp_credentials=gcp_credentials,
        project_id=project_id,
        region=region,
        repository_id=repository_id
    )
    
    final_state = monitor_workflow_invocation(
        workflow_invocation_id=workflow_invocation_id,
        gcp_credentials=gcp_credentials,
        project_id=project_id,
        region=region,
        repository_id=repository_id
    )
    
    end_result = end_task()
    
    return {
        "start": start_result,
        "config": config,
        "compilation_result_id": compilation_result_id,
        "workflow_invocation_id": workflow_invocation_id,
        "final_state": final_state,
        "end": end_result
    }


if __name__ == "__main__":
    # Local execution entry point
    # For production deployment, use Prefect CLI or UI with appropriate schedule
    # Schedule: Triggered by dataset "dataform-training-data-ingestion" completion
    # Catchup: Disabled
    # Max Parallel Width: 1
    
    dataform_transformation_pipeline(
        project_id="your-gcp-project",
        region="us-central1",
        repository_id="your-dataform-repo",
        git_commitish="main"
    )