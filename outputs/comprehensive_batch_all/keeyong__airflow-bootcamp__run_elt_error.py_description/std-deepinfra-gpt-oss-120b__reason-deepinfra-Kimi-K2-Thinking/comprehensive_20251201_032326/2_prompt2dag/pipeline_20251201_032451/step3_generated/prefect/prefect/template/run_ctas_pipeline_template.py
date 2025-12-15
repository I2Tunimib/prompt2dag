# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: run_ctas_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T03:27:36.153338
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
    description="Create Analytics Table via CTAS - run_ctas",
    retries=0,
    tags=["sequential", "docker"],
)
def run_ctas() -> Dict[str, Any]:
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
        'task_id': 'run_ctas',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Slack Failure Notification",
    description="Slack Failure Notification - slack_failure_alert",
    retries=0,
    tags=["sequential", "docker"],
)
def slack_failure_alert() -> Dict[str, Any]:
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
        'task_id': 'slack_failure_alert',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="run_ctas_pipeline",
    description="No description provided.",
    task_runner=SequentialTaskRunner(),
    log_prints=True,
)
def run_ctas_pipeline() -> Dict[str, Any]:
    """
    Main flow: run_ctas_pipeline
    
    Pattern: sequential
    Tasks: 2
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: run_ctas")
    results['run_ctas'] = run_ctas()
    print(f"Executing task: slack_failure_alert")
    results['slack_failure_alert'] = slack_failure_alert()
    
    return results


# --- Deployment Configuration ---
# No schedule configured - deploy manually or add schedule later
deployment = Deployment.build_from_flow(
    flow=run_ctas_pipeline,
    name="run_ctas_pipeline_deployment",
    work_pool_name="default-agent-pool",
    work_queue_name="default",
    tags=["sequential", "generated", "template"],
)


if __name__ == "__main__":
    # Run flow locally for testing
    run_ctas_pipeline()
    
    # To deploy:
    # deployment.apply()