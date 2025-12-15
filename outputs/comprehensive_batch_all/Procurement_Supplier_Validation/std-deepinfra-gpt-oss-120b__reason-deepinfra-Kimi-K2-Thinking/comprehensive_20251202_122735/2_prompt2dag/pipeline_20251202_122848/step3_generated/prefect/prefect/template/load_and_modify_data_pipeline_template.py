# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: load_and_modify_data_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T12:31:46.673646
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
    name="Load and Modify Supplier Data",
    description="Load and Modify Supplier Data - load_and_modify_data",
    retries=1,
    tags=["sequential", "docker"],
)
def load_and_modify_data() -> Dict[str, Any]:
    """
    Task: Load and Modify Supplier Data
    
    Executes Docker container: i2t-backendwithintertwino6-load-and-modify:latest
    """
    import subprocess
    import json
    
    # Build docker run command
    cmd = [
        'docker', 'run',
        '--rm',  # Auto-remove container
        '-v', f'{HOST_DATA_DIR}:{CONTAINER_DATA_DIR}',
        '--network', 'app_network',
    ]
    
    # Add environment variables
    cmd.extend(['-e', 'DATASET_ID=2'])
    cmd.extend(['-e', 'TABLE_NAME_PREFIX=JOT_'])
    cmd.extend(['-e', 'DATA_DIR=${DATA_DIR}'])
    
    # Add image
    cmd.append('i2t-backendwithintertwino6-load-and-modify:latest')
    
    # Add command if specified
    
    # Execute
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True
    )
    
    return {
        'task_id': 'load_and_modify_data',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Entity Reconciliation (Wikidata)",
    description="Entity Reconciliation (Wikidata) - reconcile_supplier_names",
    retries=1,
    tags=["sequential", "docker"],
)
def reconcile_supplier_names() -> Dict[str, Any]:
    """
    Task: Entity Reconciliation (Wikidata)
    
    Executes Docker container: i2t-backendwithintertwino6-reconciliation:latest
    """
    import subprocess
    import json
    
    # Build docker run command
    cmd = [
        'docker', 'run',
        '--rm',  # Auto-remove container
        '-v', f'{HOST_DATA_DIR}:{CONTAINER_DATA_DIR}',
        '--network', 'app_network',
    ]
    
    # Add environment variables
    cmd.extend(['-e', 'PRIMARY_COLUMN=supplier_name'])
    cmd.extend(['-e', 'RECONCILIATOR_ID=wikidataEntity'])
    cmd.extend(['-e', 'DATASET_ID=2'])
    cmd.extend(['-e', 'DATA_DIR=${DATA_DIR}'])
    
    # Add image
    cmd.append('i2t-backendwithintertwino6-reconciliation:latest')
    
    # Add command if specified
    
    # Execute
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True
    )
    
    return {
        'task_id': 'reconcile_supplier_names',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Save Final Validated Supplier Data",
    description="Save Final Validated Supplier Data - save_validated_data",
    retries=1,
    tags=["sequential", "docker"],
)
def save_validated_data() -> Dict[str, Any]:
    """
    Task: Save Final Validated Supplier Data
    
    Executes Docker container: i2t-backendwithintertwino6-save:latest
    """
    import subprocess
    import json
    
    # Build docker run command
    cmd = [
        'docker', 'run',
        '--rm',  # Auto-remove container
        '-v', f'{HOST_DATA_DIR}:{CONTAINER_DATA_DIR}',
        '--network', 'app_network',
    ]
    
    # Add environment variables
    cmd.extend(['-e', 'DATASET_ID=2'])
    cmd.extend(['-e', 'DATA_DIR=${DATA_DIR}'])
    
    # Add image
    cmd.append('i2t-backendwithintertwino6-save:latest')
    
    # Add command if specified
    
    # Execute
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True
    )
    
    return {
        'task_id': 'save_validated_data',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="load_and_modify_data_pipeline",
    description="No description provided.",
    task_runner=SequentialTaskRunner(),
    log_prints=True,
)
def load_and_modify_data_pipeline() -> Dict[str, Any]:
    """
    Main flow: load_and_modify_data_pipeline
    
    Pattern: sequential
    Tasks: 3
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: load_and_modify_data")
    results['load_and_modify_data'] = load_and_modify_data()
    print(f"Executing task: reconcile_supplier_names")
    results['reconcile_supplier_names'] = reconcile_supplier_names()
    print(f"Executing task: save_validated_data")
    results['save_validated_data'] = save_validated_data()
    
    return results


# --- Deployment Configuration ---
# No schedule configured - deploy manually or add schedule later
deployment = Deployment.build_from_flow(
    flow=load_and_modify_data_pipeline,
    name="load_and_modify_data_pipeline_deployment",
    work_pool_name="default-agent-pool",
    work_queue_name="default",
    tags=["sequential", "generated", "template"],
)


if __name__ == "__main__":
    # Run flow locally for testing
    load_and_modify_data_pipeline()
    
    # To deploy:
    # deployment.apply()