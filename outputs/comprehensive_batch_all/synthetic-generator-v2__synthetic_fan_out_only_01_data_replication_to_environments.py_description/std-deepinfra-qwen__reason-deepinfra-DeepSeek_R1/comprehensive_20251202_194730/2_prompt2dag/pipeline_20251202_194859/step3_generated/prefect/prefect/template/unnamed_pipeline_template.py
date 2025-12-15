# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: unnamed_pipeline
# Pattern: empty
# Strategy: template
# Generated: 2025-12-02T19:54:28.996569
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

# --- Main Flow ---
@flow(
    name="unnamed_pipeline",
    description="No description provided.",
    task_runner=ConcurrentTaskRunner(),
    log_prints=True,
)
def unnamed_pipeline() -> Dict[str, Any]:
    """
    Main flow: unnamed_pipeline
    
    Pattern: empty
    Tasks: 0
    """
    results = {}
    
    # Execute tasks in order
    
    return results


# --- Deployment Configuration ---
from prefect.server.schemas.schedules import CronSchedule

deployment = Deployment.build_from_flow(
    flow=unnamed_pipeline,
    name="unnamed_pipeline_deployment",
    work_pool_name="default-agent-pool",
    work_queue_name="default",
    schedule=CronSchedule(
        cron="@daily",
        timezone="UTC",
    ),
    tags=["empty", "generated", "template"],
)


if __name__ == "__main__":
    # Run flow locally for testing
    unnamed_pipeline()
    
    # To deploy:
    # deployment.apply()