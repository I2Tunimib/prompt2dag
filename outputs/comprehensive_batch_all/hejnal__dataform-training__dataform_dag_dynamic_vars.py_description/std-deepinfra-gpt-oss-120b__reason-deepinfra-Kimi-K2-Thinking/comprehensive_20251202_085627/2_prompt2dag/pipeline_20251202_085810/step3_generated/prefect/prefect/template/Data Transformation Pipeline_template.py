# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: Data Transformation Pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T09:02:21.672619
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
    name="Initialize Pipeline",
    description="Initialize Pipeline - initialize_pipeline",
    retries=0,
    tags=["sequential", "docker"],
)
def initialize_pipeline() -> Dict[str, Any]:
    """
    Task: Initialize Pipeline
    
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
        'task_id': 'initialize_pipeline',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Parse Input Parameters",
    description="Parse Input Parameters - parse_input_params",
    retries=0,
    tags=["sequential", "docker"],
)
def parse_input_params() -> Dict[str, Any]:
    """
    Task: Parse Input Parameters
    
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
        'task_id': 'parse_input_params',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Create Dataform Compilation Result",
    description="Create Dataform Compilation Result - create_compilation_result",
    retries=0,
    tags=["sequential", "docker"],
)
def create_compilation_result() -> Dict[str, Any]:
    """
    Task: Create Dataform Compilation Result
    
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
        'task_id': 'create_compilation_result',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Create Dataform Workflow Invocation",
    description="Create Dataform Workflow Invocation - create_workflow_invocation",
    retries=0,
    tags=["sequential", "docker"],
)
def create_workflow_invocation() -> Dict[str, Any]:
    """
    Task: Create Dataform Workflow Invocation
    
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
        'task_id': 'create_workflow_invocation',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Monitor Workflow Invocation State",
    description="Monitor Workflow Invocation State - monitor_workflow_state",
    retries=0,
    tags=["sequential", "docker"],
)
def monitor_workflow_state() -> Dict[str, Any]:
    """
    Task: Monitor Workflow Invocation State
    
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
        'task_id': 'monitor_workflow_state',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Finalize Pipeline",
    description="Finalize Pipeline - finalize_pipeline",
    retries=0,
    tags=["sequential", "docker"],
)
def finalize_pipeline() -> Dict[str, Any]:
    """
    Task: Finalize Pipeline
    
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
        'task_id': 'finalize_pipeline',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="data_transformation_pipeline",
    description="Comprehensive Pipeline Description",
    task_runner=SequentialTaskRunner(),
    log_prints=True,
)
def data_transformation_pipeline() -> Dict[str, Any]:
    """
    Main flow: Data Transformation Pipeline
    
    Pattern: sequential
    Tasks: 6
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: initialize_pipeline")
    results['initialize_pipeline'] = initialize_pipeline()
    print(f"Executing task: parse_input_params")
    results['parse_input_params'] = parse_input_params()
    print(f"Executing task: create_compilation_result")
    results['create_compilation_result'] = create_compilation_result()
    print(f"Executing task: create_workflow_invocation")
    results['create_workflow_invocation'] = create_workflow_invocation()
    print(f"Executing task: monitor_workflow_state")
    results['monitor_workflow_state'] = monitor_workflow_state()
    print(f"Executing task: finalize_pipeline")
    results['finalize_pipeline'] = finalize_pipeline()
    
    return results


# --- Deployment Configuration ---
# No schedule configured - deploy manually or add schedule later
deployment = Deployment.build_from_flow(
    flow=data_transformation_pipeline,
    name="data_transformation_pipeline_deployment",
    work_pool_name="default-agent-pool",
    work_queue_name="default",
    tags=["sequential", "generated", "template"],
)


if __name__ == "__main__":
    # Run flow locally for testing
    data_transformation_pipeline()
    
    # To deploy:
    # deployment.apply()