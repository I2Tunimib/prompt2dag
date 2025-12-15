from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import datetime
from prefect_gcp.dataform import DataformCompilationResult, DataformWorkflowInvocation, DataformWorkflowInvocationStateSensor
from prefect_gcp.credentials import GcpCredentials

# Constants
GCP_CREDENTIALS = "modelling_cloud_default"
GIT_COMMITISH = "main"  # Default Git commit or branch

@task(cache_key_fn=task_input_hash)
def start_pipeline():
    """Dummy task to initiate the pipeline execution."""
    return "Pipeline started"

@task(cache_key_fn=task_input_hash)
def parse_input_parameters(logical_date: datetime, description: str):
    """Extracts and processes runtime parameters and stores compilation configuration."""
    compilation_config = {
        "logical_date": logical_date,
        "description": description,
        "git_commitish": GIT_COMMITISH
    }
    return compilation_config

@task(cache_key_fn=task_input_hash)
def create_compilation_result(compilation_config: dict):
    """Retrieves the configuration and executes Dataform compilation."""
    gcp_credentials = GcpCredentials.load(GCP_CREDENTIALS)
    compilation_result = DataformCompilationResult(
        project="your-project-id",
        region="your-region",
        repository_id="your-repository-id",
        compilation_config=compilation_config,
        credentials=gcp_credentials
    ).run()
    return compilation_result

@task(cache_key_fn=task_input_hash)
def create_workflow_invocation(compilation_result: dict):
    """Triggers the Dataform workflow execution asynchronously."""
    gcp_credentials = GcpCredentials.load(GCP_CREDENTIALS)
    workflow_invocation = DataformWorkflowInvocation(
        project="your-project-id",
        region="your-region",
        repository_id="your-repository-id",
        compilation_result=compilation_result,
        credentials=gcp_credentials
    ).run()
    return workflow_invocation

@task(cache_key_fn=task_input_hash)
def is_workflow_invocation_done(workflow_invocation: dict):
    """Monitors the workflow invocation until it reaches either SUCCEEDED or FAILED state."""
    gcp_credentials = GcpCredentials.load(GCP_CREDENTIALS)
    DataformWorkflowInvocationStateSensor(
        project="your-project-id",
        region="your-region",
        repository_id="your-repository-id",
        workflow_invocation=workflow_invocation,
        credentials=gcp_credentials
    ).run()

@task(cache_key_fn=task_input_hash)
def end_pipeline():
    """Dummy task to mark pipeline completion."""
    return "Pipeline completed"

@flow(name="Dataform Pipeline")
def dataform_pipeline(logical_date: datetime, description: str):
    """Orchestrates the Dataform pipeline with sequential tasks."""
    start = start_pipeline()
    compilation_config = parse_input_parameters(logical_date, description)
    compilation_result = create_compilation_result(compilation_config)
    workflow_invocation = create_workflow_invocation(compilation_result)
    is_workflow_invocation_done(workflow_invocation)
    end = end_pipeline()

if __name__ == "__main__":
    # Example invocation with default parameters
    dataform_pipeline(datetime.now(), "Initial run")

# Optional: Deployment/schedule configuration
# Deployment can be configured to trigger based on dataset completion
# Example: prefect deployment build dataform_pipeline.py:DataformPipeline --name "Dataform Pipeline" --cron "0 0 * * *" --catchup False