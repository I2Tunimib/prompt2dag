# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: RunELT_Alert
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T09:27:54.913323
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
    name="Create Analytics Table via CTAS",
    description="Create Analytics Table via CTAS - create_analytics_table",
    retries=0,
    tags=["sequential", "docker"],
)
def create_analytics_table() -> Dict[str, Any]:
    """
    Task: Create Analytics Table via CTAS
    
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
        'task_id': 'create_analytics_table',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Slack Failure Notifier",
    description="Slack Failure Notifier - slack_failure_notifier",
    retries=3,
    retry_delay_seconds=30,
    tags=["sequential", "docker"],
)
def slack_failure_notifier() -> Dict[str, Any]:
    """
    Task: Slack Failure Notifier
    
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
        'task_id': 'slack_failure_notifier',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="runelt_alert",
    description="Comprehensive Pipeline Description",
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
    print(f"Executing task: create_analytics_table")
    results['create_analytics_table'] = create_analytics_table()
    print(f"Executing task: slack_failure_notifier")
    results['slack_failure_notifier'] = slack_failure_notifier()
    
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