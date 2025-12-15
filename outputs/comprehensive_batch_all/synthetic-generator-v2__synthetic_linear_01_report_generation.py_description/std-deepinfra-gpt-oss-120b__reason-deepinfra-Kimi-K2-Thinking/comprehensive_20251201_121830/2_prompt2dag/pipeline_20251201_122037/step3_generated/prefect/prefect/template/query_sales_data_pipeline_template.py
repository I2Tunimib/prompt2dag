# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: query_sales_data_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T12:23:43.941079
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
    name="Query Sales Data",
    description="Query Sales Data - query_sales_data",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def query_sales_data() -> Dict[str, Any]:
    """
    Task: Query Sales Data
    
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
        'task_id': 'query_sales_data',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Transform Sales Data to CSV",
    description="Transform Sales Data to CSV - transform_to_csv",
    retries=0,
    tags=["sequential", "docker"],
)
def transform_to_csv() -> Dict[str, Any]:
    """
    Task: Transform Sales Data to CSV
    
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
        'task_id': 'transform_to_csv',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Generate PDF Chart",
    description="Generate PDF Chart - generate_pdf_chart",
    retries=0,
    tags=["sequential", "docker"],
)
def generate_pdf_chart() -> Dict[str, Any]:
    """
    Task: Generate PDF Chart
    
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
        'task_id': 'generate_pdf_chart',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Email Sales Report",
    description="Email Sales Report - email_sales_report",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def email_sales_report() -> Dict[str, Any]:
    """
    Task: Email Sales Report
    
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
        'task_id': 'email_sales_report',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="query_sales_data_pipeline",
    description="Sequential linear pipeline that generates daily sales reports by querying PostgreSQL sales data, converting it to CSV, creating a PDF chart, and emailing the report to management.",
    task_runner=SequentialTaskRunner(),
    log_prints=True,
)
def query_sales_data_pipeline() -> Dict[str, Any]:
    """
    Main flow: query_sales_data_pipeline
    
    Pattern: sequential
    Tasks: 4
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: query_sales_data")
    results['query_sales_data'] = query_sales_data()
    print(f"Executing task: transform_to_csv")
    results['transform_to_csv'] = transform_to_csv()
    print(f"Executing task: generate_pdf_chart")
    results['generate_pdf_chart'] = generate_pdf_chart()
    print(f"Executing task: email_sales_report")
    results['email_sales_report'] = email_sales_report()
    
    return results


# --- Deployment Configuration ---
# No schedule configured - deploy manually or add schedule later
deployment = Deployment.build_from_flow(
    flow=query_sales_data_pipeline,
    name="query_sales_data_pipeline_deployment",
    work_pool_name="default-agent-pool",
    work_queue_name="default",
    tags=["sequential", "generated", "template"],
)


if __name__ == "__main__":
    # Run flow locally for testing
    query_sales_data_pipeline()
    
    # To deploy:
    # deployment.apply()