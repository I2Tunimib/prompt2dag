import os
import json
import csv
from typing import List, Dict, Any
from dagster import (
    op,
    job,
    OpExecutionContext,
    RetryPolicy,
    get_dagster_logger,
    resource,
)

# Resource for pipeline configuration
@resource
def pipeline_config_resource(context):
    """Resource providing pipeline configuration from environment variables."""
    return {
        "data_dir": os.getenv("DATA_DIR", "/app/data"),
        "here_api_token": os.getenv("HERE_API_TOKEN", "dummy_token"),
        "dataset_id": 2,
        "table_name_prefix": "JOT_",
        "service_url": "http://localhost:3003",
        "save_service_url": "http://localhost:3003",
    }

# Resource for Docker service metadata
@resource
def docker_services_resource(context):
    """Resource providing Docker service image names for reference."""
    return {
        "load_and_modify": "i2t-backendwithintertwino6-load-and-modify:latest",
        "reconciliation": "i2t-backendwithintertw