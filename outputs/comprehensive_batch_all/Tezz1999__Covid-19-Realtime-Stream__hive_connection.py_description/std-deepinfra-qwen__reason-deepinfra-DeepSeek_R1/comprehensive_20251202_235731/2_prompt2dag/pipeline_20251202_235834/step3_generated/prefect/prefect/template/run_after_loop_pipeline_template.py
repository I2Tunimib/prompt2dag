# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: run_after_loop_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-03T00:01:43.447203
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
    name="Run After Loop",
    description="Run After Loop - run_after_loop",
    retries=0,
    tags=["sequential", "docker"],
)
def run_after_loop() -> Dict[str, Any]:
    """
    Task: Run After Loop
    
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
        'task_id': 'run_after_loop',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Hive Script Task",
    description="Hive Script Task - hive_script_task",
    retries=0,
    tags=["sequential", "docker"],
)
def hive_script_task() -> Dict[str, Any]:
    """
    Task: Hive Script Task
    
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
        'task_id': 'hive_script_task',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="run_after_loop_pipeline",
    description="No description provided.",
    task_runner=SequentialTaskRunner(),
    log_prints=True,
)
def run_after_loop_pipeline() -> Dict[str, Any]:
    """
    Main flow: run_after_loop_pipeline
    
    Pattern: sequential
    Tasks: 2
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: run_after_loop")
    results['run_after_loop'] = run_after_loop()
    print(f"Executing task: hive_script_task")
    results['hive_script_task'] = hive_script_task()
    
    return results


# --- Deployment Configuration ---
# No schedule configured - deploy manually or add schedule later
deployment = Deployment.build_from_flow(
    flow=run_after_loop_pipeline,
    name="run_after_loop_pipeline_deployment",
    work_pool_name="default-agent-pool",
    work_queue_name="default",
    tags=["sequential", "generated", "template"],
)


if __name__ == "__main__":
    # Run flow locally for testing
    run_after_loop_pipeline()
    
    # To deploy:
    # deployment.apply()