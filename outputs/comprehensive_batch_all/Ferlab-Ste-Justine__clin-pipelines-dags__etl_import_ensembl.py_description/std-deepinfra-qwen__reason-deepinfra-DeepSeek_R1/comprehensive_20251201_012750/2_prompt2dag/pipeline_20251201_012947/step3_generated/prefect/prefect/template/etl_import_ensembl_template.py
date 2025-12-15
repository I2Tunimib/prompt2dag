# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: etl_import_ensembl
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T01:34:25.882456
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
    name="Check and Download Ensembl Files",
    description="Check and Download Ensembl Files - check_and_download_ensembl_files",
    retries=3,
    retry_delay_seconds=60,
    tags=["sequential", "docker"],
)
def check_and_download_ensembl_files() -> Dict[str, Any]:
    """
    Task: Check and Download Ensembl Files
    
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
        'task_id': 'check_and_download_ensembl_files',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Process Ensembl Files with Spark",
    description="Process Ensembl Files with Spark - process_ensembl_files_with_spark",
    retries=3,
    retry_delay_seconds=60,
    tags=["sequential", "docker"],
)
def process_ensembl_files_with_spark() -> Dict[str, Any]:
    """
    Task: Process Ensembl Files with Spark
    
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
        'task_id': 'process_ensembl_files_with_spark',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="etl_import_ensembl",
    description="No description provided.",
    task_runner=SequentialTaskRunner(),
    log_prints=True,
)
def etl_import_ensembl() -> Dict[str, Any]:
    """
    Main flow: etl_import_ensembl
    
    Pattern: sequential
    Tasks: 2
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: check_and_download_ensembl_files")
    results['check_and_download_ensembl_files'] = check_and_download_ensembl_files()
    print(f"Executing task: process_ensembl_files_with_spark")
    results['process_ensembl_files_with_spark'] = process_ensembl_files_with_spark()
    
    return results


# --- Deployment Configuration ---
# No schedule configured - deploy manually or add schedule later
deployment = Deployment.build_from_flow(
    flow=etl_import_ensembl,
    name="etl_import_ensembl_deployment",
    work_pool_name="default-agent-pool",
    work_queue_name="default",
    tags=["sequential", "generated", "template"],
)


if __name__ == "__main__":
    # Run flow locally for testing
    etl_import_ensembl()
    
    # To deploy:
    # deployment.apply()