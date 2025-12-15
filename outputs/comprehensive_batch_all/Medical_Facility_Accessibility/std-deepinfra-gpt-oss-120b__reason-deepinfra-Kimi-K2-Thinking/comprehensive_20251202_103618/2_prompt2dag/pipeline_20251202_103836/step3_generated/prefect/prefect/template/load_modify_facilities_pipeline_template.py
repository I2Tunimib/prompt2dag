# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: load_modify_facilities_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T10:47:26.041428
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
    name="Load and Modify Facilities Data",
    description="Load and Modify Facilities Data - load_modify_facilities",
    retries=1,
    retry_delay_seconds=60,
    tags=["sequential", "docker"],
)
def load_modify_facilities() -> Dict[str, Any]:
    """
    Task: Load and Modify Facilities Data
    
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
        'task_id': 'load_modify_facilities',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Geocode Facilities Using HERE API",
    description="Geocode Facilities Using HERE API - geocode_facilities",
    retries=1,
    retry_delay_seconds=60,
    tags=["sequential", "docker"],
)
def geocode_facilities() -> Dict[str, Any]:
    """
    Task: Geocode Facilities Using HERE API
    
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
    cmd.extend(['-e', 'PRIMARY_COLUMN=address'])
    cmd.extend(['-e', 'RECONCILIATOR_ID=geocodingHere'])
    cmd.extend(['-e', 'API_TOKEN=[HERE API token]'])
    cmd.extend(['-e', 'DATASET_ID=2'])
    
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
        'task_id': 'geocode_facilities',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Calculate Distance to Nearest Public Transport",
    description="Calculate Distance to Nearest Public Transport - calculate_distance_to_public_transport",
    retries=1,
    retry_delay_seconds=60,
    tags=["sequential", "docker"],
)
def calculate_distance_to_public_transport() -> Dict[str, Any]:
    """
    Task: Calculate Distance to Nearest Public Transport
    
    Executes Docker container: i2t-backendwithintertwino6-column-extension:latest
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
    cmd.extend(['-e', 'EXTENDER_ID=spatialDistanceCalculator'])
    cmd.extend(['-e', 'LAT_COLUMN=latitude'])
    cmd.extend(['-e', 'LON_COLUMN=longitude'])
    cmd.extend(['-e', 'TARGET_LAYER=public_transport'])
    cmd.extend(['-e', 'TARGET_DATA_SOURCE=/app/data/transport_stops.geojson'])
    cmd.extend(['-e', 'OUTPUT_COLUMN=distance_to_pt'])
    cmd.extend(['-e', 'DATASET_ID=2'])
    
    # Add image
    cmd.append('i2t-backendwithintertwino6-column-extension:latest')
    
    # Add command if specified
    
    # Execute
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True
    )
    
    return {
        'task_id': 'calculate_distance_to_public_transport',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Calculate Distance to Nearest Residential Area",
    description="Calculate Distance to Nearest Residential Area - calculate_distance_to_residential_areas",
    retries=1,
    retry_delay_seconds=60,
    tags=["sequential", "docker"],
)
def calculate_distance_to_residential_areas() -> Dict[str, Any]:
    """
    Task: Calculate Distance to Nearest Residential Area
    
    Executes Docker container: i2t-backendwithintertwino6-column-extension:latest
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
    cmd.extend(['-e', 'EXTENDER_ID=spatialDistanceCalculator'])
    cmd.extend(['-e', 'LAT_COLUMN=latitude'])
    cmd.extend(['-e', 'LON_COLUMN=longitude'])
    cmd.extend(['-e', 'TARGET_LAYER=residential_areas'])
    cmd.extend(['-e', 'TARGET_DATA_SOURCE=/app/data/residential_areas.geojson'])
    cmd.extend(['-e', 'OUTPUT_COLUMN=distance_to_residential'])
    cmd.extend(['-e', 'DATASET_ID=2'])
    
    # Add image
    cmd.append('i2t-backendwithintertwino6-column-extension:latest')
    
    # Add command if specified
    
    # Execute
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True
    )
    
    return {
        'task_id': 'calculate_distance_to_residential_areas',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Save Enriched Facility Accessibility Data",
    description="Save Enriched Facility Accessibility Data - save_facilities_accessibility",
    retries=1,
    retry_delay_seconds=60,
    tags=["sequential", "docker"],
)
def save_facilities_accessibility() -> Dict[str, Any]:
    """
    Task: Save Enriched Facility Accessibility Data
    
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
        'task_id': 'save_facilities_accessibility',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="load_modify_facilities_pipeline",
    description="No description provided.",
    task_runner=SequentialTaskRunner(),
    log_prints=True,
)
def load_modify_facilities_pipeline() -> Dict[str, Any]:
    """
    Main flow: load_modify_facilities_pipeline
    
    Pattern: sequential
    Tasks: 5
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: load_modify_facilities")
    results['load_modify_facilities'] = load_modify_facilities()
    print(f"Executing task: geocode_facilities")
    results['geocode_facilities'] = geocode_facilities()
    print(f"Executing task: calculate_distance_to_public_transport")
    results['calculate_distance_to_public_transport'] = calculate_distance_to_public_transport()
    print(f"Executing task: calculate_distance_to_residential_areas")
    results['calculate_distance_to_residential_areas'] = calculate_distance_to_residential_areas()
    print(f"Executing task: save_facilities_accessibility")
    results['save_facilities_accessibility'] = save_facilities_accessibility()
    
    return results


# --- Deployment Configuration ---
# No schedule configured - deploy manually or add schedule later
deployment = Deployment.build_from_flow(
    flow=load_modify_facilities_pipeline,
    name="load_modify_facilities_pipeline_deployment",
    work_pool_name="default-agent-pool",
    work_queue_name="default",
    tags=["sequential", "generated", "template"],
)


if __name__ == "__main__":
    # Run flow locally for testing
    load_modify_facilities_pipeline()
    
    # To deploy:
    # deployment.apply()