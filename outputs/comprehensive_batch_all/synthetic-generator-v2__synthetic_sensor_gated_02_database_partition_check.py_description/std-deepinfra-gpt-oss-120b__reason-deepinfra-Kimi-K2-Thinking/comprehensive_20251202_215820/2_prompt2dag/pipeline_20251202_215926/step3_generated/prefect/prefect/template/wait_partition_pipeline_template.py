# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: wait_partition_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T22:02:13.146106
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
    name="Wait for Daily Orders Partition",
    description="Wait for Daily Orders Partition - wait_partition",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def wait_partition() -> Dict[str, Any]:
    """
    Task: Wait for Daily Orders Partition
    
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
    name="Load Orders to Data Warehouse",
    description="Load Orders to Data Warehouse - load",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def load() -> Dict[str, Any]:
    """
    Task: Load Orders to Data Warehouse
    
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
    description="No description provided.",
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
# No schedule configured - deploy manually or add schedule later
deployment = Deployment.build_from_flow(
    flow=wait_partition_pipeline,
    name="wait_partition_pipeline_deployment",
    work_pool_name="default-agent-pool",
    work_queue_name="default",
    tags=["sequential", "generated", "template"],
)


if __name__ == "__main__":
    # Run flow locally for testing
    wait_partition_pipeline()
    
    # To deploy:
    # deployment.apply()