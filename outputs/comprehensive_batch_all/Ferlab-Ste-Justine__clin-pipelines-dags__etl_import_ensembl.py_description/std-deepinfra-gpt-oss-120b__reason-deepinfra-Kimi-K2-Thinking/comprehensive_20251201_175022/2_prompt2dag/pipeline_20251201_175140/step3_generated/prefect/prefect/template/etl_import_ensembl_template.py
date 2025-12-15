# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: etl_import_ensembl
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T17:55:09.729662
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
    name="Extract Ensembl Mapping Files",
    description="Extract Ensembl Mapping Files - extract_ensembl_files",
    retries=0,
    tags=["sequential", "docker"],
)
def extract_ensembl_files() -> Dict[str, Any]:
    """
    Task: Extract Ensembl Mapping Files
    
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
        'task_id': 'extract_ensembl_files',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Transform Ensembl Mapping with Spark",
    description="Transform Ensembl Mapping with Spark - transform_ensembl_mapping",
    retries=0,
    tags=["sequential", "docker"],
)
def transform_ensembl_mapping() -> Dict[str, Any]:
    """
    Task: Transform Ensembl Mapping with Spark
    
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
        'task_id': 'transform_ensembl_mapping',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="etl_import_ensembl",
    description="Comprehensive Pipeline Description",
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
    print(f"Executing task: extract_ensembl_files")
    results['extract_ensembl_files'] = extract_ensembl_files()
    print(f"Executing task: transform_ensembl_mapping")
    results['transform_ensembl_mapping'] = transform_ensembl_mapping()
    
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