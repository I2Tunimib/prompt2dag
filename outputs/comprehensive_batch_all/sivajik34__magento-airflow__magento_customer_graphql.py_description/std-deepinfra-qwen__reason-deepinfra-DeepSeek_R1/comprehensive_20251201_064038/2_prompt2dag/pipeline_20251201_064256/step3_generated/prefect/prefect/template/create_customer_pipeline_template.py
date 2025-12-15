# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: create_customer_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T06:48:27.975874
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
    name="Create Customer",
    description="Create Customer - create_customer",
    retries=1,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def create_customer() -> Dict[str, Any]:
    """
    Task: Create Customer
    
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
        'task_id': 'create_customer',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Generate Customer Token",
    description="Generate Customer Token - generate_customer_token",
    retries=1,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def generate_customer_token() -> Dict[str, Any]:
    """
    Task: Generate Customer Token
    
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
        'task_id': 'generate_customer_token',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Get Customer Info",
    description="Get Customer Info - get_customer_info",
    retries=1,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def get_customer_info() -> Dict[str, Any]:
    """
    Task: Get Customer Info
    
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
        'task_id': 'get_customer_info',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="create_customer_pipeline",
    description="No description provided.",
    task_runner=SequentialTaskRunner(),
    log_prints=True,
)
def create_customer_pipeline() -> Dict[str, Any]:
    """
    Main flow: create_customer_pipeline
    
    Pattern: sequential
    Tasks: 3
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: create_customer")
    results['create_customer'] = create_customer()
    print(f"Executing task: generate_customer_token")
    results['generate_customer_token'] = generate_customer_token()
    print(f"Executing task: get_customer_info")
    results['get_customer_info'] = get_customer_info()
    
    return results


# --- Deployment Configuration ---
# No schedule configured - deploy manually or add schedule later
deployment = Deployment.build_from_flow(
    flow=create_customer_pipeline,
    name="create_customer_pipeline_deployment",
    work_pool_name="default-agent-pool",
    work_queue_name="default",
    tags=["sequential", "generated", "template"],
)


if __name__ == "__main__":
    # Run flow locally for testing
    create_customer_pipeline()
    
    # To deploy:
    # deployment.apply()