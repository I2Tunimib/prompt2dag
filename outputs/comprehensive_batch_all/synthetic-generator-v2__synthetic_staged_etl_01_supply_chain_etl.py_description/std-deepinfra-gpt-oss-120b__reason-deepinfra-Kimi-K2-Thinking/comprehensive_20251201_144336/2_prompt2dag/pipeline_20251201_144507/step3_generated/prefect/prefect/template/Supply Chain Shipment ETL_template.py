# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: Supply Chain Shipment ETL
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T14:49:38.158130
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
    name="Cleanse and Normalize Shipments",
    description="Cleanse and Normalize Shipments - cleanse_and_normalize_shipments",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def cleanse_and_normalize_shipments() -> Dict[str, Any]:
    """
    Task: Cleanse and Normalize Shipments
    
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
    name="Load Shipments to Inventory Database",
    description="Load Shipments to Inventory Database - load_shipments_to_inventory",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def load_shipments_to_inventory() -> Dict[str, Any]:
    """
    Task: Load Shipments to Inventory Database
    
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
        'task_id': 'load_shipments_to_inventory',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Send ETL Summary Email",
    description="Send ETL Summary Email - send_etl_summary_email",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def send_etl_summary_email() -> Dict[str, Any]:
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
        'task_id': 'send_etl_summary_email',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="supply_chain_shipment_etl",
    description="No description provided.",
    task_runner=ConcurrentTaskRunner(),
    log_prints=True,
)
def supply_chain_shipment_etl() -> Dict[str, Any]:
    """
    Main flow: Supply Chain Shipment ETL
    
    Pattern: sequential
    Tasks: 3
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: cleanse_and_normalize_shipments")
    results['cleanse_and_normalize_shipments'] = cleanse_and_normalize_shipments()
    print(f"Executing task: load_shipments_to_inventory")
    results['load_shipments_to_inventory'] = load_shipments_to_inventory()
    print(f"Executing task: send_etl_summary_email")
    results['send_etl_summary_email'] = send_etl_summary_email()
    
    return results


# --- Deployment Configuration ---
# No schedule configured - deploy manually or add schedule later
deployment = Deployment.build_from_flow(
    flow=supply_chain_shipment_etl,
    name="supply_chain_shipment_etl_deployment",
    work_pool_name="default-agent-pool",
    work_queue_name="default",
    tags=["sequential", "generated", "template"],
)


if __name__ == "__main__":
    # Run flow locally for testing
    supply_chain_shipment_etl()
    
    # To deploy:
    # deployment.apply()