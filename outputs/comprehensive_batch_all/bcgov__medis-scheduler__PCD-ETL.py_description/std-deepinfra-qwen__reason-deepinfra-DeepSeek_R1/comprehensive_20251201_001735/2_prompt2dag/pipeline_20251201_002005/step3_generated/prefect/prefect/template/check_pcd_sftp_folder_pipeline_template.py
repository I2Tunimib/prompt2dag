# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: check_pcd_sftp_folder_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T00:29:36.027795
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
    name="Start PCD Extract 1",
    description="Start PCD Extract 1 - start_pcd_extract_1",
    retries=0,
    tags=["sequential", "docker"],
)
def start_pcd_extract_1() -> Dict[str, Any]:
    """
    Task: Start PCD Extract 1
    
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
        'task_id': 'start_pcd_extract_1',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Parallel HTTP API Extraction",
    description="Parallel HTTP API Extraction - parallel_http_api_extraction",
    retries=0,
    tags=["sequential", "docker"],
)
def parallel_http_api_extraction() -> Dict[str, Any]:
    """
    Task: Parallel HTTP API Extraction
    
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
        'task_id': 'parallel_http_api_extraction',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Start PCD Extract 2",
    description="Start PCD Extract 2 - start_pcd_extract_2",
    retries=0,
    tags=["sequential", "docker"],
)
def start_pcd_extract_2() -> Dict[str, Any]:
    """
    Task: Start PCD Extract 2
    
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
        'task_id': 'start_pcd_extract_2',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="PCD File Upload",
    description="PCD File Upload - pcd_file_upload",
    retries=0,
    tags=["sequential", "docker"],
)
def pcd_file_upload() -> Dict[str, Any]:
    """
    Task: PCD File Upload
    
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
        'task_id': 'pcd_file_upload',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="ETL Notification",
    description="ETL Notification - etl_notification",
    retries=0,
    tags=["sequential", "docker"],
)
def etl_notification() -> Dict[str, Any]:
    """
    Task: ETL Notification
    
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
        'task_id': 'etl_notification',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="check_pcd_sftp_folder_pipeline",
    description="Comprehensive Pipeline Description",
    task_runner=ConcurrentTaskRunner(),
    log_prints=True,
)
def check_pcd_sftp_folder_pipeline() -> Dict[str, Any]:
    """
    Main flow: check_pcd_sftp_folder_pipeline
    
    Pattern: sequential
    Tasks: 7
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: check_pcd_sftp_folder")
    results['check_pcd_sftp_folder'] = check_pcd_sftp_folder()
    print(f"Executing task: check_pcd_shared_folder")
    results['check_pcd_shared_folder'] = check_pcd_shared_folder()
    print(f"Executing task: start_pcd_extract_1")
    results['start_pcd_extract_1'] = start_pcd_extract_1()
    print(f"Executing task: parallel_http_api_extraction")
    results['parallel_http_api_extraction'] = parallel_http_api_extraction()
    print(f"Executing task: start_pcd_extract_2")
    results['start_pcd_extract_2'] = start_pcd_extract_2()
    print(f"Executing task: pcd_file_upload")
    results['pcd_file_upload'] = pcd_file_upload()
    print(f"Executing task: etl_notification")
    results['etl_notification'] = etl_notification()
    
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