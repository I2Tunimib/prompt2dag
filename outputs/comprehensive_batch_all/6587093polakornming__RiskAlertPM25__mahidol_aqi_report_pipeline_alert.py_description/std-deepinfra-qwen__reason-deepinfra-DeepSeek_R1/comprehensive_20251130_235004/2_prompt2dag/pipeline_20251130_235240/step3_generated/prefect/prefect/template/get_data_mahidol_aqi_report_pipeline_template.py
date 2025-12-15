# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: get_data_mahidol_aqi_report_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T00:01:00.562692
# ==============================================================================

from __future__ import annotations

import os
from datetime import timedelta
from typing import Dict, Any

from prefect import flow, task
from prefect.infrastructure.docker import DockerContainer
from prefect.deployments import Deployment
from prefect.task_runners import SequentialTaskRunner

# --- Configuration ---
HOST_DATA_DIR = os.getenv('HOST_DATA_DIR', '/tmp/prefect/data')
CONTAINER_DATA_DIR = '/app/data'

# --- Task Definitions ---

@task(
    name="Scrape Mahidol AQI Report",
    description="Scrape Mahidol AQI Report - get_data_mahidol_aqi_report",
    retries=0,
    tags=["sequential", "docker"],
)
def get_data_mahidol_aqi_report() -> Dict[str, Any]:
    """
    Task: Scrape Mahidol AQI Report
    
    Executes Docker container: 
    """
    import subprocess
    import json
    
    # Build docker run command
    cmd = [
        'docker', 'run',
        '--rm',  # Auto-remove container
        '-v', f'{HOST_DATA_DIR}:{CONTAINER_DATA_DIR}',
    ]
    
    # Add environment variables
    
    # Add image
    cmd.append('')
    
    # Add command if specified
    
    # Execute
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True
    )
    
    return {
        'task_id': 'get_data_mahidol_aqi_report',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Parse and Validate AQI Data",
    description="Parse and Validate AQI Data - create_json_object",
    retries=0,
    tags=["sequential", "docker"],
)
def create_json_object() -> Dict[str, Any]:
    """
    Task: Parse and Validate AQI Data
    
    Executes Docker container: 
    """
    import subprocess
    import json
    
    # Build docker run command
    cmd = [
        'docker', 'run',
        '--rm',  # Auto-remove container
        '-v', f'{HOST_DATA_DIR}:{CONTAINER_DATA_DIR}',
    ]
    
    # Add environment variables
    
    # Add image
    cmd.append('')
    
    # Add command if specified
    
    # Execute
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True
    )
    
    return {
        'task_id': 'create_json_object',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Load AQI Data to PostgreSQL",
    description="Load AQI Data to PostgreSQL - load_mahidol_aqi_to_postgres",
    retries=0,
    tags=["sequential", "docker"],
)
def load_mahidol_aqi_to_postgres() -> Dict[str, Any]:
    """
    Task: Load AQI Data to PostgreSQL
    
    Executes Docker container: 
    """
    import subprocess
    import json
    
    # Build docker run command
    cmd = [
        'docker', 'run',
        '--rm',  # Auto-remove container
        '-v', f'{HOST_DATA_DIR}:{CONTAINER_DATA_DIR}',
    ]
    
    # Add environment variables
    
    # Add image
    cmd.append('')
    
    # Add command if specified
    
    # Execute
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True
    )
    
    return {
        'task_id': 'load_mahidol_aqi_to_postgres',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Send AQI Alert Emails",
    description="Send AQI Alert Emails - alert_email",
    retries=0,
    tags=["sequential", "docker"],
)
def alert_email() -> Dict[str, Any]:
    """
    Task: Send AQI Alert Emails
    
    Executes Docker container: 
    """
    import subprocess
    import json
    
    # Build docker run command
    cmd = [
        'docker', 'run',
        '--rm',  # Auto-remove container
        '-v', f'{HOST_DATA_DIR}:{CONTAINER_DATA_DIR}',
    ]
    
    # Add environment variables
    
    # Add image
    cmd.append('')
    
    # Add command if specified
    
    # Execute
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True
    )
    
    return {
        'task_id': 'alert_email',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="get_data_mahidol_aqi_report_pipeline",
    description="No description provided.",
    task_runner=SequentialTaskRunner(),
    log_prints=True,
)
def get_data_mahidol_aqi_report_pipeline() -> Dict[str, Any]:
    """
    Main flow: get_data_mahidol_aqi_report_pipeline
    
    Pattern: sequential
    Tasks: 4
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: get_data_mahidol_aqi_report")
    results['get_data_mahidol_aqi_report'] = get_data_mahidol_aqi_report()
    print(f"Executing task: create_json_object")
    results['create_json_object'] = create_json_object()
    print(f"Executing task: load_mahidol_aqi_to_postgres")
    results['load_mahidol_aqi_to_postgres'] = load_mahidol_aqi_to_postgres()
    print(f"Executing task: alert_email")
    results['alert_email'] = alert_email()
    
    return results


# --- Deployment Configuration ---
# No schedule configured - deploy manually or add schedule later
deployment = Deployment.build_from_flow(
    flow=get_data_mahidol_aqi_report_pipeline,
    name="get_data_mahidol_aqi_report_pipeline_deployment",
    work_pool_name="default-agent-pool",
    work_queue_name="default",
    tags=["sequential", "generated", "template"],
)


if __name__ == "__main__":
    # Run flow locally for testing
    get_data_mahidol_aqi_report_pipeline()
    
    # To deploy:
    # deployment.apply()