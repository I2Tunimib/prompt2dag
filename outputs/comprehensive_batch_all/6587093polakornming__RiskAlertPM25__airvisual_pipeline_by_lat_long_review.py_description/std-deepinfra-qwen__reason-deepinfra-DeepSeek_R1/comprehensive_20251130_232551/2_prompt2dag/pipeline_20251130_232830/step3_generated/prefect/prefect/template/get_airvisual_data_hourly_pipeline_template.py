# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: get_airvisual_data_hourly_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-11-30T23:33:27.522541
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
    name="Fetch AirVisual Data",
    description="Fetch AirVisual Data - get_airvisual_data_hourly",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def get_airvisual_data_hourly() -> Dict[str, Any]:
    """
    Task: Fetch AirVisual Data
    
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
        'task_id': 'get_airvisual_data_hourly',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Read and Validate AirVisual Data",
    description="Read and Validate AirVisual Data - read_data_airvisual",
    retries=2,
    retry_delay_seconds=180,
    tags=["sequential", "docker"],
)
def read_data_airvisual() -> Dict[str, Any]:
    """
    Task: Read and Validate AirVisual Data
    
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
        'task_id': 'read_data_airvisual',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Load AirVisual Data to PostgreSQL",
    description="Load AirVisual Data to PostgreSQL - load_data_airvisual_to_postgresql",
    retries=2,
    retry_delay_seconds=180,
    tags=["sequential", "docker"],
)
def load_data_airvisual_to_postgresql() -> Dict[str, Any]:
    """
    Task: Load AirVisual Data to PostgreSQL
    
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
        'task_id': 'load_data_airvisual_to_postgresql',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="get_airvisual_data_hourly_pipeline",
    description="No description provided.",
    task_runner=SequentialTaskRunner(),
    log_prints=True,
)
def get_airvisual_data_hourly_pipeline() -> Dict[str, Any]:
    """
    Main flow: get_airvisual_data_hourly_pipeline
    
    Pattern: sequential
    Tasks: 3
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: get_airvisual_data_hourly")
    results['get_airvisual_data_hourly'] = get_airvisual_data_hourly()
    print(f"Executing task: read_data_airvisual")
    results['read_data_airvisual'] = read_data_airvisual()
    print(f"Executing task: load_data_airvisual_to_postgresql")
    results['load_data_airvisual_to_postgresql'] = load_data_airvisual_to_postgresql()
    
    return results


# --- Deployment Configuration ---
# No schedule configured - deploy manually or add schedule later
deployment = Deployment.build_from_flow(
    flow=get_airvisual_data_hourly_pipeline,
    name="get_airvisual_data_hourly_pipeline_deployment",
    work_pool_name="default-agent-pool",
    work_queue_name="default",
    tags=["sequential", "generated", "template"],
)


if __name__ == "__main__":
    # Run flow locally for testing
    get_airvisual_data_hourly_pipeline()
    
    # To deploy:
    # deployment.apply()