# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: Database Partition Check ETL
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T13:38:13.978107
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
    name="Wait for Daily Partition",
    description="Wait for Daily Partition - wait_partition",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def wait_partition() -> Dict[str, Any]:
    """
    Task: Wait for Daily Partition
    
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
    description="Transform Orders Data - transform_orders",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def transform_orders() -> Dict[str, Any]:
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
        'task_id': 'transform_orders',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Load Orders to Warehouse",
    description="Load Orders to Warehouse - load_orders",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def load_orders() -> Dict[str, Any]:
    """
    Task: Load Orders to Warehouse
    
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
        'task_id': 'load_orders',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="database_partition_check_etl",
    description="Sensor-gated daily ETL pipeline that waits for database partition availability before extracting, transforming, and loading incremental orders data.",
    task_runner=SequentialTaskRunner(),
    log_prints=True,
)
def database_partition_check_etl() -> Dict[str, Any]:
    """
    Main flow: Database Partition Check ETL
    
    Pattern: sequential
    Tasks: 4
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: wait_partition")
    results['wait_partition'] = wait_partition()
    print(f"Executing task: extract_incremental")
    results['extract_incremental'] = extract_incremental()
    print(f"Executing task: transform_orders")
    results['transform_orders'] = transform_orders()
    print(f"Executing task: load_orders")
    results['load_orders'] = load_orders()
    
    return results


# --- Deployment Configuration ---
from prefect.server.schemas.schedules import CronSchedule

deployment = Deployment.build_from_flow(
    flow=database_partition_check_etl,
    name="database_partition_check_etl_deployment",
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
    database_partition_check_etl()
    
    # To deploy:
    # deployment.apply()