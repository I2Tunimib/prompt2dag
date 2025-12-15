# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: check_pcd_sftp_folder_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T00:40:49.789898
# ==============================================================================

from __future__ import annotations

import os
from datetime import timedelta
from typing import Dict, Any

from prefect import flow, task
from prefect.infrastructure.docker import DockerContainer
from prefect.deployments import Deployment
from prefect.task_runners import ConcurrentTaskRunner

# --- Configuration ---
HOST_DATA_DIR = os.getenv('HOST_DATA_DIR', '/tmp/prefect/data')
CONTAINER_DATA_DIR = '/app/data'

# --- Task Definitions ---

@task(
    name="Check PCD SFTP Folder",
    description="Check PCD SFTP Folder - check_pcd_sftp_folder",
    retries=0,
    tags=["sequential", "docker"],
)
def check_pcd_sftp_folder() -> Dict[str, Any]:
    """
    Task: Check PCD SFTP Folder
    
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
        'task_id': 'check_pcd_sftp_folder',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Check PCD Shared Folder",
    description="Check PCD Shared Folder - check_pcd_shared_folder",
    retries=0,
    tags=["sequential", "docker"],
)
def check_pcd_shared_folder() -> Dict[str, Any]:
    """
    Task: Check PCD Shared Folder
    
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
        'task_id': 'check_pcd_shared_folder',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Extract PCD API Data",
    description="Extract PCD API Data - extract_pcd_api",
    retries=3,
    retry_delay_seconds=30,
    tags=["sequential", "docker"],
)
def extract_pcd_api() -> Dict[str, Any]:
    """
    Task: Extract PCD API Data
    
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
        'task_id': 'extract_pcd_api',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Process and Load PCD Data",
    description="Process and Load PCD Data - process_and_load_pcd_data",
    retries=2,
    retry_delay_seconds=60,
    tags=["sequential", "docker"],
)
def process_and_load_pcd_data() -> Dict[str, Any]:
    """
    Task: Process and Load PCD Data
    
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
        'task_id': 'process_and_load_pcd_data',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Send ETL Notification",
    description="Send ETL Notification - send_etl_notification",
    retries=1,
    tags=["sequential", "docker"],
)
def send_etl_notification() -> Dict[str, Any]:
    """
    Task: Send ETL Notification
    
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
        'task_id': 'send_etl_notification',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="check_pcd_sftp_folder_pipeline",
    description="Staged ETL pipeline for Primary Care Data (PCD) processing with folder validation, parallel API extraction, Kubernetes‑based processing and email notification.",
    task_runner=ConcurrentTaskRunner(),
    log_prints=True,
)
def check_pcd_sftp_folder_pipeline() -> Dict[str, Any]:
    """
    Main flow: check_pcd_sftp_folder_pipeline
    
    Pattern: sequential
    Tasks: 5
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: check_pcd_sftp_folder")
    results['check_pcd_sftp_folder'] = check_pcd_sftp_folder()
    print(f"Executing task: check_pcd_shared_folder")
    results['check_pcd_shared_folder'] = check_pcd_shared_folder()
    print(f"Executing task: extract_pcd_api")
    results['extract_pcd_api'] = extract_pcd_api()
    print(f"Executing task: process_and_load_pcd_data")
    results['process_and_load_pcd_data'] = process_and_load_pcd_data()
    print(f"Executing task: send_etl_notification")
    results['send_etl_notification'] = send_etl_notification()
    
    return results


# --- Deployment Configuration ---
# No schedule configured - deploy manually or add schedule later
deployment = Deployment.build_from_flow(
    flow=check_pcd_sftp_folder_pipeline,
    name="check_pcd_sftp_folder_pipeline_deployment",
    work_pool_name="default-agent-pool",
    work_queue_name="default",
    tags=["sequential", "generated", "template"],
)


if __name__ == "__main__":
    # Run flow locally for testing
    check_pcd_sftp_folder_pipeline()
    
    # To deploy:
    # deployment.apply()