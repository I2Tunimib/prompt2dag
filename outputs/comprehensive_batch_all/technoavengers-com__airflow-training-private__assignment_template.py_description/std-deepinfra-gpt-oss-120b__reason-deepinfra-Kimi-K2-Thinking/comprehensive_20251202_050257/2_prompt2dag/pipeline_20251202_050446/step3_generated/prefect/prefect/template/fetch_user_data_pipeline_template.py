# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: fetch_user_data_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T05:08:28.901312
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
    name="Fetch User Data from API",
    description="Fetch User Data from API - fetch_user_data",
    retries=0,
    tags=["sequential", "docker"],
)
def fetch_user_data() -> Dict[str, Any]:
    """
    Task: Fetch User Data from API
    
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
        'task_id': 'fetch_user_data',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Transform User Data",
    description="Transform User Data - transform_user_data",
    retries=0,
    tags=["sequential", "docker"],
)
def transform_user_data() -> Dict[str, Any]:
    """
    Task: Transform User Data
    
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
        'task_id': 'transform_user_data',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Create Users Table in PostgreSQL",
    description="Create Users Table in PostgreSQL - create_user_table",
    retries=0,
    tags=["sequential", "docker"],
)
def create_user_table() -> Dict[str, Any]:
    """
    Task: Create Users Table in PostgreSQL
    
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
        'task_id': 'create_user_table',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Insert User Data into PostgreSQL",
    description="Insert User Data into PostgreSQL - insert_user_data",
    retries=0,
    tags=["sequential", "docker"],
)
def insert_user_data() -> Dict[str, Any]:
    """
    Task: Insert User Data into PostgreSQL
    
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
        'task_id': 'insert_user_data',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="fetch_user_data_pipeline",
    description="No description provided.",
    task_runner=SequentialTaskRunner(),
    log_prints=True,
)
def fetch_user_data_pipeline() -> Dict[str, Any]:
    """
    Main flow: fetch_user_data_pipeline
    
    Pattern: sequential
    Tasks: 4
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: fetch_user_data")
    results['fetch_user_data'] = fetch_user_data()
    print(f"Executing task: transform_user_data")
    results['transform_user_data'] = transform_user_data()
    print(f"Executing task: create_user_table")
    results['create_user_table'] = create_user_table()
    print(f"Executing task: insert_user_data")
    results['insert_user_data'] = insert_user_data()
    
    return results


# --- Deployment Configuration ---
from prefect.server.schemas.schedules import CronSchedule

deployment = Deployment.build_from_flow(
    flow=fetch_user_data_pipeline,
    name="fetch_user_data_pipeline_deployment",
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
    fetch_user_data_pipeline()
    
    # To deploy:
    # deployment.apply()