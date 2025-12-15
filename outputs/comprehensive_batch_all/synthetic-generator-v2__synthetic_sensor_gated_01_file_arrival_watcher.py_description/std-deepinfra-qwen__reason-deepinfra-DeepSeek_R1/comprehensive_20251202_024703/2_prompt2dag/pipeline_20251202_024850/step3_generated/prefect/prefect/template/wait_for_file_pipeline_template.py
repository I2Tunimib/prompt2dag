# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: wait_for_file_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T02:52:30.818275
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
    name="Wait for File",
    description="Wait for File - wait_for_file",
    retries=0,
    retry_delay_seconds=30,
    tags=["sequential", "docker"],
)
def wait_for_file() -> Dict[str, Any]:
    """
    Task: Wait for File
    
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
        'task_id': 'wait_for_file',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Validate Schema",
    description="Validate Schema - validate_schema",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def validate_schema() -> Dict[str, Any]:
    """
    Task: Validate Schema
    
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
        'task_id': 'validate_schema',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Load Database",
    description="Load Database - load_db",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def load_db() -> Dict[str, Any]:
    """
    Task: Load Database
    
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
        'task_id': 'load_db',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="wait_for_file_pipeline",
    description="No description provided.",
    task_runner=SequentialTaskRunner(),
    log_prints=True,
)
def wait_for_file_pipeline() -> Dict[str, Any]:
    """
    Main flow: wait_for_file_pipeline
    
    Pattern: sequential
    Tasks: 3
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: wait_for_file")
    results['wait_for_file'] = wait_for_file()
    print(f"Executing task: validate_schema")
    results['validate_schema'] = validate_schema()
    print(f"Executing task: load_db")
    results['load_db'] = load_db()
    
    return results


# --- Deployment Configuration ---
from prefect.server.schemas.schedules import CronSchedule

deployment = Deployment.build_from_flow(
    flow=wait_for_file_pipeline,
    name="wait_for_file_pipeline_deployment",
    work_pool_name="default-agent-pool",
    work_queue_name="default",
    schedule=CronSchedule(
        cron="@daily",
        timezone="UTC",
    ),
    tags=["sequential", "generated", "template"],
)


if __name__ == "__main__":
    # Run flow locally for testing
    wait_for_file_pipeline()
    
    # To deploy:
    # deployment.apply()