# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: wait_for_ftp_file_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T13:59:59.367995
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
    name="Wait for FTP File",
    description="Wait for FTP File - wait_for_ftp_file",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def wait_for_ftp_file() -> Dict[str, Any]:
    """
    Task: Wait for FTP File
    
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
        'task_id': 'wait_for_ftp_file',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Download Vendor File",
    description="Download Vendor File - download_vendor_file",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def download_vendor_file() -> Dict[str, Any]:
    """
    Task: Download Vendor File
    
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
        'task_id': 'download_vendor_file',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Cleanse Vendor Data",
    description="Cleanse Vendor Data - cleanse_vendor_data",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def cleanse_vendor_data() -> Dict[str, Any]:
    """
    Task: Cleanse Vendor Data
    
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
        'task_id': 'cleanse_vendor_data',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Merge with Internal Inventory",
    description="Merge with Internal Inventory - merge_with_internal_inventory",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def merge_with_internal_inventory() -> Dict[str, Any]:
    """
    Task: Merge with Internal Inventory
    
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
        'task_id': 'merge_with_internal_inventory',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="wait_for_ftp_file_pipeline",
    description="No description provided.",
    task_runner=SequentialTaskRunner(),
    log_prints=True,
)
def wait_for_ftp_file_pipeline() -> Dict[str, Any]:
    """
    Main flow: wait_for_ftp_file_pipeline
    
    Pattern: sequential
    Tasks: 4
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: wait_for_ftp_file")
    results['wait_for_ftp_file'] = wait_for_ftp_file()
    print(f"Executing task: download_vendor_file")
    results['download_vendor_file'] = download_vendor_file()
    print(f"Executing task: cleanse_vendor_data")
    results['cleanse_vendor_data'] = cleanse_vendor_data()
    print(f"Executing task: merge_with_internal_inventory")
    results['merge_with_internal_inventory'] = merge_with_internal_inventory()
    
    return results


# --- Deployment Configuration ---
from prefect.server.schemas.schedules import CronSchedule

deployment = Deployment.build_from_flow(
    flow=wait_for_ftp_file_pipeline,
    name="wait_for_ftp_file_pipeline_deployment",
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
    wait_for_ftp_file_pipeline()
    
    # To deploy:
    # deployment.apply()