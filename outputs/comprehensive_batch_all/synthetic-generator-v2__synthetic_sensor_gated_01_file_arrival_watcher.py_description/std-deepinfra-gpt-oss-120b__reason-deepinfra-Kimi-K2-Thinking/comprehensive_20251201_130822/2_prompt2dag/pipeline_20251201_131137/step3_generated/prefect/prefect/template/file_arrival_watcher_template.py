# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: file_arrival_watcher
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T13:14:13.449746
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
    name="Wait for Transaction File",
    description="Wait for Transaction File - wait_for_file",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def wait_for_file() -> Dict[str, Any]:
    """
    Task: Wait for Transaction File
    
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
    name="Validate Transaction File Schema",
    description="Validate Transaction File Schema - validate_schema",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def validate_schema() -> Dict[str, Any]:
    """
    Task: Validate Transaction File Schema
    
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
    name="Load Validated Transactions to PostgreSQL",
    description="Load Validated Transactions to PostgreSQL - load_db",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def load_db() -> Dict[str, Any]:
    """
    Task: Load Validated Transactions to PostgreSQL
    
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
    name="file_arrival_watcher",
    description="Monitors daily transaction file arrivals, validates schema, and loads data into PostgreSQL.",
    task_runner=SequentialTaskRunner(),
    log_prints=True,
)
def file_arrival_watcher() -> Dict[str, Any]:
    """
    Main flow: file_arrival_watcher
    
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
    flow=file_arrival_watcher,
    name="file_arrival_watcher_deployment",
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
    file_arrival_watcher()
    
    # To deploy:
    # deployment.apply()