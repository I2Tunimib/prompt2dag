# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: PM2.5_Risk_Alert_Pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T16:19:23.360296
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
    name="Extract Mahidol AQI HTML",
    description="Extract Mahidol AQI HTML - extract_mahidol_aqi_html",
    retries=0,
    tags=["sequential", "docker"],
)
def extract_mahidol_aqi_html() -> Dict[str, Any]:
    """
    Task: Extract Mahidol AQI HTML
    
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
        'task_id': 'extract_mahidol_aqi_html',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Transform HTML to Structured JSON",
    description="Transform HTML to Structured JSON - transform_html_to_json",
    retries=0,
    tags=["sequential", "docker"],
)
def transform_html_to_json() -> Dict[str, Any]:
    """
    Task: Transform HTML to Structured JSON
    
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
        'task_id': 'transform_html_to_json',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Load AQI Data to PostgreSQL Warehouse",
    description="Load AQI Data to PostgreSQL Warehouse - load_mahidol_aqi_to_warehouse",
    retries=0,
    tags=["sequential", "docker"],
)
def load_mahidol_aqi_to_warehouse() -> Dict[str, Any]:
    """
    Task: Load AQI Data to PostgreSQL Warehouse
    
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
        'task_id': 'load_mahidol_aqi_to_warehouse',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="PM2.5 Email Alert Notification",
    description="PM2.5 Email Alert Notification - notify_pm25_alert",
    retries=0,
    tags=["sequential", "docker"],
)
def notify_pm25_alert() -> Dict[str, Any]:
    """
    Task: PM2.5 Email Alert Notification
    
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
        'task_id': 'notify_pm25_alert',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="pm2.5_risk_alert_pipeline",
    description="Sequential ETL pipeline that scrapes Mahidol University AQI data, transforms it to JSON, loads it into PostgreSQL, and sends email alerts when PM2.5 exceeds thresholds.",
    task_runner=SequentialTaskRunner(),
    log_prints=True,
)
def pm2.5_risk_alert_pipeline() -> Dict[str, Any]:
    """
    Main flow: PM2.5_Risk_Alert_Pipeline
    
    Pattern: sequential
    Tasks: 4
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: extract_mahidol_aqi_html")
    results['extract_mahidol_aqi_html'] = extract_mahidol_aqi_html()
    print(f"Executing task: transform_html_to_json")
    results['transform_html_to_json'] = transform_html_to_json()
    print(f"Executing task: load_mahidol_aqi_to_warehouse")
    results['load_mahidol_aqi_to_warehouse'] = load_mahidol_aqi_to_warehouse()
    print(f"Executing task: notify_pm25_alert")
    results['notify_pm25_alert'] = notify_pm25_alert()
    
    return results


# --- Deployment Configuration ---
# No schedule configured - deploy manually or add schedule later
deployment = Deployment.build_from_flow(
    flow=pm2.5_risk_alert_pipeline,
    name="pm2.5_risk_alert_pipeline_deployment",
    work_pool_name="default-agent-pool",
    work_queue_name="default",
    tags=["sequential", "generated", "template"],
)


if __name__ == "__main__":
    # Run flow locally for testing
    pm2.5_risk_alert_pipeline()
    
    # To deploy:
    # deployment.apply()