# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: [Pipeline Name]
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T05:22:39.510683
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
    name="Run System Check",
    description="Run System Check - run_system_check",
    retries=0,
    tags=["sequential", "docker"],
)
def run_system_check() -> Dict[str, Any]:
    """
    Task: Run System Check
    
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
        'task_id': 'run_system_check',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Run Hive Script",
    description="Run Hive Script - run_hive_script",
    retries=0,
    tags=["sequential", "docker"],
)
def run_hive_script() -> Dict[str, Any]:
    """
    Task: Run Hive Script
    
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
        'task_id': 'run_hive_script',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="[pipeline_name]",
    description="Simple linear data pipeline that executes Hive database operations for COVID-19 realtime streaming data.",
    task_runner=SequentialTaskRunner(),
    log_prints=True,
)
def [pipeline_name]() -> Dict[str, Any]:
    """
    Main flow: [Pipeline Name]
    
    Pattern: sequential
    Tasks: 2
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: run_system_check")
    results['run_system_check'] = run_system_check()
    print(f"Executing task: run_hive_script")
    results['run_hive_script'] = run_hive_script()
    
    return results


# --- Deployment Configuration ---
from prefect.server.schemas.schedules import CronSchedule

deployment = Deployment.build_from_flow(
    flow=[pipeline_name],
    name="[pipeline_name]_deployment",
    work_pool_name="default-agent-pool",
    work_queue_name="default",
    schedule=CronSchedule(
        cron="00 1 * * *",
        timezone="UTC",
    ),
    tags=["sequential", "generated", "template"],
)


if __name__ == "__main__":
    # Run flow locally for testing
    [pipeline_name]()
    
    # To deploy:
    # deployment.apply()