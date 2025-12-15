# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: RunELT_Alert
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T19:42:04.065694
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
    name="Run CTAS to Build Analytics Tables",
    description="Run CTAS to Build Analytics Tables - run_ctas",
    retries=0,
    tags=["sequential", "docker"],
)
def run_ctas() -> Dict[str, Any]:
    """
    Task: Run CTAS to Build Analytics Tables
    
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
        'task_id': 'run_ctas',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Slack Failure Notification",
    description="Slack Failure Notification - notify_failure_slack",
    retries=3,
    retry_delay_seconds=60,
    tags=["sequential", "docker"],
)
def notify_failure_slack() -> Dict[str, Any]:
    """
    Task: Slack Failure Notification
    
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
        'task_id': 'notify_failure_slack',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="runelt_alert",
    description="Sequential ELT pipeline that builds analytics tables in Snowflake using CTAS operations, with data validation, atomic swaps, and Slack failure notifications.",
    task_runner=SequentialTaskRunner(),
    log_prints=True,
)
def runelt_alert() -> Dict[str, Any]:
    """
    Main flow: RunELT_Alert
    
    Pattern: sequential
    Tasks: 2
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: notify_failure_slack")
    results['notify_failure_slack'] = notify_failure_slack()
    print(f"Executing task: run_ctas")
    results['run_ctas'] = run_ctas()
    
    return results


# --- Deployment Configuration ---
from prefect.server.schemas.schedules import CronSchedule

deployment = Deployment.build_from_flow(
    flow=runelt_alert,
    name="runelt_alert_deployment",
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
    runelt_alert()
    
    # To deploy:
    # deployment.apply()