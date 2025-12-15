# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: Airflow Database Cleanup
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T11:44:54.019250
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
    name="Load Cleanup Configuration",
    description="Load Cleanup Configuration - load_cleanup_configuration",
    retries=1,
    retry_delay_seconds=60,
    tags=["sequential", "docker"],
)
def load_cleanup_configuration() -> Dict[str, Any]:
    """
    Task: Load Cleanup Configuration
    
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
        'task_id': 'load_cleanup_configuration',
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
    name="airflow_database_cleanup",
    description="No description provided.",
    task_runner=SequentialTaskRunner(),
    log_prints=True,
)
def airflow_database_cleanup() -> Dict[str, Any]:
    """
    Main flow: Airflow Database Cleanup
    
    Pattern: sequential
    Tasks: 2
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: load_cleanup_configuration")
    results['load_cleanup_configuration'] = load_cleanup_configuration()
    print(f"Executing task: cleanup_airflow_metadb")
    results['cleanup_airflow_metadb'] = cleanup_airflow_metadb()
    
    return results


# --- Deployment Configuration ---
from prefect.server.schemas.schedules import CronSchedule

deployment = Deployment.build_from_flow(
    flow=airflow_database_cleanup,
    name="airflow_database_cleanup_deployment",
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
    airflow_database_cleanup()
    
    # To deploy:
    # deployment.apply()