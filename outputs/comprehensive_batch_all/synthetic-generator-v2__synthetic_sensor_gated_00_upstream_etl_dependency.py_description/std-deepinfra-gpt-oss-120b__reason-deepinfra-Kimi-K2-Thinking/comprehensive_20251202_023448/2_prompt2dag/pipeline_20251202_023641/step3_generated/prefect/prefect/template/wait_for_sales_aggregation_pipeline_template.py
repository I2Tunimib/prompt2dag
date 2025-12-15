# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: wait_for_sales_aggregation_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T02:39:50.875899
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
    name="Wait for Sales Aggregation DAG",
    description="Wait for Sales Aggregation DAG - wait_for_sales_aggregation",
    retries=0,
    tags=["sequential", "docker"],
)
def wait_for_sales_aggregation() -> Dict[str, Any]:
    """
    Task: Wait for Sales Aggregation DAG
    
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
        'task_id': 'wait_for_sales_aggregation',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Load Aggregated Sales CSV",
    description="Load Aggregated Sales CSV - load_sales_csv",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def load_sales_csv() -> Dict[str, Any]:
    """
    Task: Load Aggregated Sales CSV
    
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
        'task_id': 'load_sales_csv',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Generate Executive Dashboard",
    description="Generate Executive Dashboard - generate_dashboard",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def generate_dashboard() -> Dict[str, Any]:
    """
    Task: Generate Executive Dashboard
    
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
        'task_id': 'generate_dashboard',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="wait_for_sales_aggregation_pipeline",
    description="No description provided.",
    task_runner=SequentialTaskRunner(),
    log_prints=True,
)
def wait_for_sales_aggregation_pipeline() -> Dict[str, Any]:
    """
    Main flow: wait_for_sales_aggregation_pipeline
    
    Pattern: sequential
    Tasks: 3
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: wait_for_sales_aggregation")
    results['wait_for_sales_aggregation'] = wait_for_sales_aggregation()
    print(f"Executing task: load_sales_csv")
    results['load_sales_csv'] = load_sales_csv()
    print(f"Executing task: generate_dashboard")
    results['generate_dashboard'] = generate_dashboard()
    
    return results


# --- Deployment Configuration ---
from prefect.server.schemas.schedules import CronSchedule

deployment = Deployment.build_from_flow(
    flow=wait_for_sales_aggregation_pipeline,
    name="wait_for_sales_aggregation_pipeline_deployment",
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
    wait_for_sales_aggregation_pipeline()
    
    # To deploy:
    # deployment.apply()