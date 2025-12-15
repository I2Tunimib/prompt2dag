# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: load_and_modify_data_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T07:09:52.242925
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
    name="Load and Modify Data",
    description="Load and Modify Data - load_and_modify_data",
    retries=1,
    retry_delay_seconds=60,
    tags=["sequential", "docker"],
)
def load_and_modify_data() -> Dict[str, Any]:
    """
    Task: Load and Modify Data
    
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
    cmd.extend(['-e', 'DATE_COLUMN=installation_date'])
    cmd.extend(['-e', 'TABLE_NAME_PREFIX=JOT_'])
    
    # Add image
    cmd.append('i2t-backendwithintertwino6-load-and-modify:latest')
    
    # Add command if specified
    cmd.extend(["python", "load_and_modify.py"])
    
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
    name="Reconcile Geocoding",
    description="Reconcile Geocoding - reconcile_geocoding",
    retries=1,
    retry_delay_seconds=60,
    tags=["sequential", "docker"],
)
def reconcile_geocoding() -> Dict[str, Any]:
    """
    Task: Reconcile Geocoding
    
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
    cmd.extend(['-e', 'PRIMARY_COLUMN=location'])
    cmd.extend(['-e', 'RECONCILIATOR_ID=geocodingHere'])
    cmd.extend(['-e', 'API_TOKEN=[HERE API token]'])
    cmd.extend(['-e', 'DATASET_ID=2'])
    
    # Add image
    cmd.append('i2t-backendwithintertwino6-reconciliation:latest')
    
    # Add command if specified
    cmd.extend(["python", "reconcile_geocoding.py"])
    
    # Execute
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True
    )
    
    return {
        'task_id': 'reconcile_geocoding',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Extend OpenMeteo Data",
    description="Extend OpenMeteo Data - extend_openmeteo_data",
    retries=1,
    retry_delay_seconds=60,
    tags=["sequential", "docker"],
)
def extend_openmeteo_data() -> Dict[str, Any]:
    """
    Task: Extend OpenMeteo Data
    
    Executes Docker container: i2t-backendwithintertwino6-openmeteo-extension:latest
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
    cmd.extend(['-e', 'LAT_COLUMN=latitude'])
    cmd.extend(['-e', 'LON_COLUMN=longitude'])
    cmd.extend(['-e', 'DATE_COLUMN=installation_date'])
    cmd.extend(['-e', 'WEATHER_VARIABLES=apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours'])
    cmd.extend(['-e', 'DATE_SEPARATOR_FORMAT=YYYYMMDD'])
    
    # Add image
    cmd.append('i2t-backendwithintertwino6-openmeteo-extension:latest')
    
    # Add command if specified
    cmd.extend(["python", "extend_openmeteo.py"])
    
    # Execute
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True
    )
    
    return {
        'task_id': 'extend_openmeteo_data',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Extend Land Use",
    description="Extend Land Use - extend_land_use",
    retries=1,
    retry_delay_seconds=60,
    tags=["sequential", "docker"],
)
def extend_land_use() -> Dict[str, Any]:
    """
    Task: Extend Land Use
    
    Executes Docker container: geoapify-land-use:latest
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
    cmd.extend(['-e', 'LAT_COLUMN=latitude'])
    cmd.extend(['-e', 'LON_COLUMN=longitude'])
    cmd.extend(['-e', 'OUTPUT_COLUMN=land_use_type'])
    cmd.extend(['-e', 'API_KEY=[Geoapify API key]'])
    
    # Add image
    cmd.append('geoapify-land-use:latest')
    
    # Add command if specified
    cmd.extend(["python", "extend_land_use.py"])
    
    # Execute
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True
    )
    
    return {
        'task_id': 'extend_land_use',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Extend Population Density",
    description="Extend Population Density - extend_population_density",
    retries=1,
    retry_delay_seconds=60,
    tags=["sequential", "docker"],
)
def extend_population_density() -> Dict[str, Any]:
    """
    Task: Extend Population Density
    
    Executes Docker container: worldpop-density:latest
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
    cmd.extend(['-e', 'LAT_COLUMN=latitude'])
    cmd.extend(['-e', 'LON_COLUMN=longitude'])
    cmd.extend(['-e', 'OUTPUT_COLUMN=population_density'])
    cmd.extend(['-e', 'RADIUS=5000'])
    
    # Add image
    cmd.append('worldpop-density:latest')
    
    # Add command if specified
    cmd.extend(["python", "extend_population_density.py"])
    
    # Execute
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True
    )
    
    return {
        'task_id': 'extend_population_density',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Extend Environmental Risk",
    description="Extend Environmental Risk - extend_environmental_risk",
    retries=1,
    retry_delay_seconds=60,
    tags=["sequential", "docker"],
)
def extend_environmental_risk() -> Dict[str, Any]:
    """
    Task: Extend Environmental Risk
    
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
    cmd.extend(['-e', 'EXTENDER_ID=environmentalRiskCalculator'])
    cmd.extend(['-e', 'INPUT_COLUMNS=precipitation_sum,population_density,land_use_type'])
    cmd.extend(['-e', 'OUTPUT_COLUMN=risk_score'])
    cmd.extend(['-e', 'CALCULATION_FORMULA=[risk calculation parameters]'])
    
    # Add image
    cmd.append('i2t-backendwithintertwino6-column-extension:latest')
    
    # Add command if specified
    cmd.extend(["python", "extend_environmental_risk.py"])
    
    # Execute
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True
    )
    
    return {
        'task_id': 'extend_environmental_risk',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Save Final Data",
    description="Save Final Data - save_final_data",
    retries=1,
    retry_delay_seconds=60,
    tags=["sequential", "docker"],
)
def save_final_data() -> Dict[str, Any]:
    """
    Task: Save Final Data
    
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
    cmd.extend(["python", "save_final_data.py"])
    
    # Execute
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True
    )
    
    return {
        'task_id': 'save_final_data',
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
    Tasks: 7
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: load_and_modify_data")
    results['load_and_modify_data'] = load_and_modify_data()
    print(f"Executing task: reconcile_geocoding")
    results['reconcile_geocoding'] = reconcile_geocoding()
    print(f"Executing task: extend_openmeteo_data")
    results['extend_openmeteo_data'] = extend_openmeteo_data()
    print(f"Executing task: extend_land_use")
    results['extend_land_use'] = extend_land_use()
    print(f"Executing task: extend_population_density")
    results['extend_population_density'] = extend_population_density()
    print(f"Executing task: extend_environmental_risk")
    results['extend_environmental_risk'] = extend_environmental_risk()
    print(f"Executing task: save_final_data")
    results['save_final_data'] = save_final_data()
    
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