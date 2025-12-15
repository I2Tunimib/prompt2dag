from prefect import flow, task, get_run_logger
from prefect.context import get_run_context
from google.cloud import dataform_v1beta1
from google.oauth2 import service_account
import os
import time
from datetime import datetime
from typing import Dict, Any


def get_dataform_client():
    """Initialize and return Dataform client with credentials."""
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if credentials_path:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path
        )
    else:
        credentials = None
    return dataform_v1beta1.DataformClient(credentials=credentials)


@task(retries=0)
def start_task():
    """Initiates the pipeline execution."""
    logger = get_run_logger()
    logger.info("Pipeline execution started")


@task(retries=0)
def parse_input_parameters(
    git_commitish: str,
    project_id: str,
    region: str,
    repository: str,
    description: str,
) -> Dict[str, Any]:
    """
    Extracts and processes runtime parameters including logical date and description,
    then stores compilation configuration for downstream tasks.
    """
    logger = get_run_logger()
    context = get_run_context()
    logical_date = context.flow_run.expected_start_time
    config = {
        "logical_date": logical_date.isoformat(),
        "description": description,
        "git_commitish": git_commitish,
        "project_id": project_id,
        "region": region,
        "repository": repository,
    }
    logger.info(f"Parsed compilation configuration: {config}")
    return config


@task(retries=0)
def create_compilation_result(config: Dict[str, Any]) -> str:
    """
    Retrieves the configuration and executes Dataform compilation
    using the parsed parameters.
    """
    logger = get_run_logger()
    client = get_dataform_client()
    project_id = config["project_id"]
    region = config["region"]
    repository = config["repository"]
    git_commitish = config["git_commitish"]
    repository_path = f"projects/{project_id}/locations/{region}/repositories/{repository}"
    compilation_result = dataform_v1beta1.CompilationResult(
        git_commitish=git_commitish,
    )
    request = dataform_v1beta1.CreateCompilationResultRequest(
        parent=repository_path,
        compilation_result=compilation_result,
    )
    response = client.create_compilation_result(request=request)
    compilation_result_name = response.name
    logger.info(f"Created compilation result: {compilation_result_name}")
    return compilation_result_name


@task(retries=0)
def create_workflow_invocation(compilation_result_name: str, config: Dict[str, Any]) -> str:
    """
    Triggers the Dataform workflow execution asynchronously using the compilation result.
    """
    logger = get_run_logger()
    client = get_dataform_client()
    parts = compilation_result_name.split("/")
    project_id = parts[1]
    region = parts[3]
    repository = parts[5]
    repository_path = f"projects/{project_id}/locations/{region}/repositories/{repository}"
    workflow_invocation