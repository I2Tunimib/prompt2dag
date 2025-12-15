# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: fetch_brokerage_holdings_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T19:17:56.874957
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
    name="Fetch Brokerage Holdings",
    description="Fetch Brokerage Holdings - fetch_brokerage_holdings",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def fetch_brokerage_holdings() -> Dict[str, Any]:
    """
    Task: Fetch Brokerage Holdings
    
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
        'task_id': 'fetch_brokerage_holdings',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Analyze Portfolio",
    description="Analyze Portfolio - analyze_portfolio",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def analyze_portfolio() -> Dict[str, Any]:
    """
    Task: Analyze Portfolio
    
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
        'task_id': 'analyze_portfolio',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Aggregate and Rebalance",
    description="Aggregate and Rebalance - aggregate_and_rebalance",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def aggregate_and_rebalance() -> Dict[str, Any]:
    """
    Task: Aggregate and Rebalance
    
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
        'task_id': 'aggregate_and_rebalance',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Generate Trade Orders",
    description="Generate Trade Orders - generate_trade_orders",
    retries=2,
    retry_delay_seconds=300,
    tags=["sequential", "docker"],
)
def generate_trade_orders() -> Dict[str, Any]:
    """
    Task: Generate Trade Orders
    
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
        'task_id': 'generate_trade_orders',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="fetch_brokerage_holdings_pipeline",
    description="Portfolio Rebalancing DAG",
    task_runner=ConcurrentTaskRunner(),
    log_prints=True,
)
def fetch_brokerage_holdings_pipeline() -> Dict[str, Any]:
    """
    Main flow: fetch_brokerage_holdings_pipeline
    
    Pattern: sequential
    Tasks: 4
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: fetch_brokerage_holdings")
    results['fetch_brokerage_holdings'] = fetch_brokerage_holdings()
    print(f"Executing task: analyze_portfolio")
    results['analyze_portfolio'] = analyze_portfolio()
    print(f"Executing task: aggregate_and_rebalance")
    results['aggregate_and_rebalance'] = aggregate_and_rebalance()
    print(f"Executing task: generate_trade_orders")
    results['generate_trade_orders'] = generate_trade_orders()
    
    return results


# --- Deployment Configuration ---
from prefect.server.schemas.schedules import CronSchedule

deployment = Deployment.build_from_flow(
    flow=fetch_brokerage_holdings_pipeline,
    name="fetch_brokerage_holdings_pipeline_deployment",
    work_pool_name="default-agent-pool",
    work_queue_name="default",
    schedule=CronSchedule(
        cron="@daily",
        timezone="UTC",
    ),
    tags=["sequential", "generated", "template"],
)


if __name__ == "__main__":
    # Run flow locally for testing
    fetch_brokerage_holdings_pipeline()
    
    # To deploy:
    # deployment.apply()