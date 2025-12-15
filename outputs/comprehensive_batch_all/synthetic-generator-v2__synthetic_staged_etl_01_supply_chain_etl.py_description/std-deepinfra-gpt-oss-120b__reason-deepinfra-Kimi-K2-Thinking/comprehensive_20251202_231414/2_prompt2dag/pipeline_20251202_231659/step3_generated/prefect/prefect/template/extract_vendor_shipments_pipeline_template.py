# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: extract_vendor_shipments_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T23:22:52.623435
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
    name="Extract Vendor Shipment CSVs",
    description="Extract Vendor Shipment CSVs - extract_vendor_shipments",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def extract_vendor_shipments() -> Dict[str, Any]:
    """
    Task: Extract Vendor Shipment CSVs
    
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
        'task_id': 'extract_vendor_shipments',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Cleanse and Normalize Shipment Data",
    description="Cleanse and Normalize Shipment Data - cleanse_and_normalize_shipments",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def cleanse_and_normalize_shipments() -> Dict[str, Any]:
    """
    Task: Cleanse and Normalize Shipment Data
    
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
        'task_id': 'cleanse_and_normalize_shipments',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Load Cleansed Shipments to Inventory Database",
    description="Load Cleansed Shipments to Inventory Database - load_shipment_data",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def load_shipment_data() -> Dict[str, Any]:
    """
    Task: Load Cleansed Shipments to Inventory Database
    
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
        'task_id': 'load_shipment_data',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Send ETL Summary Email",
    description="Send ETL Summary Email - send_summary_email",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def send_summary_email() -> Dict[str, Any]:
    """
    Task: Send ETL Summary Email
    
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
        'task_id': 'send_summary_email',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="extract_vendor_shipments_pipeline",
    description="No description provided.",
    task_runner=ConcurrentTaskRunner(),
    log_prints=True,
)
def extract_vendor_shipments_pipeline() -> Dict[str, Any]:
    """
    Main flow: extract_vendor_shipments_pipeline
    
    Pattern: sequential
    Tasks: 4
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: send_summary_email")
    results['send_summary_email'] = send_summary_email()
    print(f"Executing task: load_shipment_data")
    results['load_shipment_data'] = load_shipment_data()
    print(f"Executing task: cleanse_and_normalize_shipments")
    results['cleanse_and_normalize_shipments'] = cleanse_and_normalize_shipments()
    print(f"Executing task: extract_vendor_shipments")
    results['extract_vendor_shipments'] = extract_vendor_shipments()
    
    return results


# --- Deployment Configuration ---
# No schedule configured - deploy manually or add schedule later
deployment = Deployment.build_from_flow(
    flow=extract_vendor_shipments_pipeline,
    name="extract_vendor_shipments_pipeline_deployment",
    work_pool_name="default-agent-pool",
    work_queue_name="default",
    tags=["sequential", "generated", "template"],
)


if __name__ == "__main__":
    # Run flow locally for testing
    extract_vendor_shipments_pipeline()
    
    # To deploy:
    # deployment.apply()