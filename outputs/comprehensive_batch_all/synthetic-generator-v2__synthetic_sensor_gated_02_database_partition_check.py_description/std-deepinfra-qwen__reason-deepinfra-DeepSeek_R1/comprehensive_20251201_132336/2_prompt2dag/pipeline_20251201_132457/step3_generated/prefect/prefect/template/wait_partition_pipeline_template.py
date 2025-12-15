# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: wait_partition_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T13:28:28.096437
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
    name="Wait for Partition",
    description="Wait for Partition - wait_partition",
    retries=0,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def wait_partition() -> Dict[str, Any]:
    """
    Task: Wait for Partition
    
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
        'task_id': 'wait_partition',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Extract Incremental Orders",
    description="Extract Incremental Orders - extract_incremental",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def extract_incremental() -> Dict[str, Any]:
    """
    Task: Extract Incremental Orders
    
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
        'task_id': 'extract_incremental',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Transform Orders Data",
    description="Transform Orders Data - transform",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def transform() -> Dict[str, Any]:
    """
    Task: Transform Orders Data
    
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
        'task_id': 'transform',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Load Orders Data",
    description="Load Orders Data - load",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def load() -> Dict[str, Any]:
    """
    Task: Load Orders Data
    
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
        'task_id': 'load',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="wait_partition_pipeline",
    description="Comprehensive Pipeline Description: This is a sensor-gated daily ETL pipeline that waits for database partition availability before extracting, transforming, and loading incremental orders data.",
    task_runner=SequentialTaskRunner(),
    log_prints=True,
)
def wait_partition_pipeline() -> Dict[str, Any]:
    """
    Main flow: wait_partition_pipeline
    
    Pattern: sequential
    Tasks: 4
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: wait_partition")
    results['wait_partition'] = wait_partition()
    print(f"Executing task: extract_incremental")
    results['extract_incremental'] = extract_incremental()
    print(f"Executing task: transform")
    results['transform'] = transform()
    print(f"Executing task: load")
    results['load'] = load()
    
    return results


# --- Deployment Configuration ---
from prefect.server.schemas.schedules import CronSchedule

deployment = Deployment.build_from_flow(
    flow=wait_partition_pipeline,
    name="wait_partition_pipeline_deployment",
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
    wait_partition_pipeline()
    
    # To deploy:
    # deployment.apply()