# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: etl_import_mondo
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T18:17:08.603892
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
    name="Validate Parameters",
    description="Validate Parameters - params_validate",
    retries=0,
    tags=["sequential", "docker"],
)
def params_validate() -> Dict[str, Any]:
    """
    Task: Validate Parameters
    
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
        'task_id': 'params_validate',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Download Mondo Terms",
    description="Download Mondo Terms - download_mondo_terms",
    retries=0,
    tags=["sequential", "docker"],
)
def download_mondo_terms() -> Dict[str, Any]:
    """
    Task: Download Mondo Terms
    
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
        'task_id': 'download_mondo_terms',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Normalize Mondo Terms",
    description="Normalize Mondo Terms - normalized_mondo_terms",
    retries=0,
    tags=["sequential", "docker"],
)
def normalized_mondo_terms() -> Dict[str, Any]:
    """
    Task: Normalize Mondo Terms
    
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
        'task_id': 'normalized_mondo_terms',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Index Mondo Terms",
    description="Index Mondo Terms - index_mondo_terms",
    retries=0,
    tags=["sequential", "docker"],
)
def index_mondo_terms() -> Dict[str, Any]:
    """
    Task: Index Mondo Terms
    
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
        'task_id': 'index_mondo_terms',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Publish Mondo Data",
    description="Publish Mondo Data - publish_mondo",
    retries=0,
    tags=["sequential", "docker"],
)
def publish_mondo() -> Dict[str, Any]:
    """
    Task: Publish Mondo Data
    
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
        'task_id': 'publish_mondo',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Send Slack Notification",
    description="Send Slack Notification - slack",
    retries=0,
    tags=["sequential", "docker"],
)
def slack() -> Dict[str, Any]:
    """
    Task: Send Slack Notification
    
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
        'task_id': 'slack',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="etl_import_mondo",
    description="No description provided.",
    task_runner=SequentialTaskRunner(),
    log_prints=True,
)
def etl_import_mondo() -> Dict[str, Any]:
    """
    Main flow: etl_import_mondo
    
    Pattern: sequential
    Tasks: 6
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: params_validate")
    results['params_validate'] = params_validate()
    print(f"Executing task: download_mondo_terms")
    results['download_mondo_terms'] = download_mondo_terms()
    print(f"Executing task: normalized_mondo_terms")
    results['normalized_mondo_terms'] = normalized_mondo_terms()
    print(f"Executing task: index_mondo_terms")
    results['index_mondo_terms'] = index_mondo_terms()
    print(f"Executing task: publish_mondo")
    results['publish_mondo'] = publish_mondo()
    print(f"Executing task: slack")
    results['slack'] = slack()
    
    return results


# --- Deployment Configuration ---
# No schedule configured - deploy manually or add schedule later
deployment = Deployment.build_from_flow(
    flow=etl_import_mondo,
    name="etl_import_mondo_deployment",
    work_pool_name="default-agent-pool",
    work_queue_name="default",
    tags=["sequential", "generated", "template"],
)


if __name__ == "__main__":
    # Run flow locally for testing
    etl_import_mondo()
    
    # To deploy:
    # deployment.apply()