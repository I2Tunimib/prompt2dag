# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: print_configuration_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T05:15:09.691558
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
    name="Print Configuration",
    description="Print Configuration - print_configuration",
    retries=1,
    retry_delay_seconds=60,
    tags=["sequential", "docker"],
)
def print_configuration() -> Dict[str, Any]:
    """
    Task: Print Configuration
    
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
        'task_id': 'print_configuration',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Cleanup Airflow MetaDB",
    description="Cleanup Airflow MetaDB - cleanup_airflow_metadb",
    retries=1,
    retry_delay_seconds=60,
    tags=["sequential", "docker"],
)
def cleanup_airflow_metadb() -> Dict[str, Any]:
    """
    Task: Cleanup Airflow MetaDB
    
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
        'task_id': 'cleanup_airflow_metadb',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="print_configuration_pipeline",
    description="No description provided.",
    task_runner=SequentialTaskRunner(),
    log_prints=True,
)
def print_configuration_pipeline() -> Dict[str, Any]:
    """
    Main flow: print_configuration_pipeline
    
    Pattern: sequential
    Tasks: 2
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: print_configuration")
    results['print_configuration'] = print_configuration()
    print(f"Executing task: cleanup_airflow_metadb")
    results['cleanup_airflow_metadb'] = cleanup_airflow_metadb()
    
    return results


# --- Deployment Configuration ---
from prefect.server.schemas.schedules import CronSchedule

deployment = Deployment.build_from_flow(
    flow=print_configuration_pipeline,
    name="print_configuration_pipeline_deployment",
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
    print_configuration_pipeline()
    
    # To deploy:
    # deployment.apply()