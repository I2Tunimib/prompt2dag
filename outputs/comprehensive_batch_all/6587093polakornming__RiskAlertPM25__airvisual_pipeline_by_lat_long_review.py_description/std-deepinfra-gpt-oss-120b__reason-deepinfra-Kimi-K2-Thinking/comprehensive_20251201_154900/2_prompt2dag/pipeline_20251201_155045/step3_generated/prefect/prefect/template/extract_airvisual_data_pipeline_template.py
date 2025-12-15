# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: extract_airvisual_data_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T15:53:43.650971
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
    name="Extract AirVisual Data",
    description="Extract AirVisual Data - extract_airvisual_data",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def extract_airvisual_data() -> Dict[str, Any]:
    """
    Task: Extract AirVisual Data
    
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
        'task_id': 'extract_airvisual_data',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Validate AirVisual JSON",
    description="Validate AirVisual JSON - validate_airvisual_json",
    retries=2,
    retry_delay_seconds=180,
    tags=["sequential", "docker"],
)
def validate_airvisual_json() -> Dict[str, Any]:
    """
    Task: Validate AirVisual JSON
    
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
        'task_id': 'validate_airvisual_json',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Load AirVisual Data to PostgreSQL",
    description="Load AirVisual Data to PostgreSQL - load_airvisual_to_postgresql",
    retries=2,
    retry_delay_seconds=180,
    tags=["sequential", "docker"],
)
def load_airvisual_to_postgresql() -> Dict[str, Any]:
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
        'task_id': 'load_airvisual_to_postgresql',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="extract_airvisual_data_pipeline",
    description="No description provided.",
    task_runner=SequentialTaskRunner(),
    log_prints=True,
)
def extract_airvisual_data_pipeline() -> Dict[str, Any]:
    """
    Main flow: extract_airvisual_data_pipeline
    
    Pattern: sequential
    Tasks: 3
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: extract_airvisual_data")
    results['extract_airvisual_data'] = extract_airvisual_data()
    print(f"Executing task: validate_airvisual_json")
    results['validate_airvisual_json'] = validate_airvisual_json()
    print(f"Executing task: load_airvisual_to_postgresql")
    results['load_airvisual_to_postgresql'] = load_airvisual_to_postgresql()
    
    return results


# --- Deployment Configuration ---
# No schedule configured - deploy manually or add schedule later
deployment = Deployment.build_from_flow(
    flow=extract_airvisual_data_pipeline,
    name="extract_airvisual_data_pipeline_deployment",
    work_pool_name="default-agent-pool",
    work_queue_name="default",
    tags=["sequential", "generated", "template"],
)


if __name__ == "__main__":
    # Run flow locally for testing
    extract_airvisual_data_pipeline()
    
    # To deploy:
    # deployment.apply()