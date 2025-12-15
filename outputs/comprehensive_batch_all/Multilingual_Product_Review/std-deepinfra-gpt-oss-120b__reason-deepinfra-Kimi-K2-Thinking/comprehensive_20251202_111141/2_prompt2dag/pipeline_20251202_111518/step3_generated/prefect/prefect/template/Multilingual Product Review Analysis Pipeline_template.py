# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: Multilingual Product Review Analysis Pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T11:18:25.657955
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
    name="Load and Modify Reviews",
    description="Load and Modify Reviews - load_and_modify_reviews",
    retries=1,
    tags=["sequential", "docker"],
)
def load_and_modify_reviews() -> Dict[str, Any]:
    """
    Task: Load and Modify Reviews
    
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
        'task_id': 'load_and_modify_reviews',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Detect Review Language",
    description="Detect Review Language - detect_review_language",
    retries=1,
    tags=["sequential", "docker"],
)
def detect_review_language() -> Dict[str, Any]:
    """
    Task: Detect Review Language
    
    Executes Docker container: jmockit/language-detection
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
    
    # Add image
    cmd.append('jmockit/language-detection')
    
    # Add command if specified
    
    # Execute
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True
    )
    
    return {
        'task_id': 'detect_review_language',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Sentiment Analysis",
    description="Sentiment Analysis - analyze_sentiment",
    retries=1,
    tags=["sequential", "docker"],
)
def analyze_sentiment() -> Dict[str, Any]:
    """
    Task: Sentiment Analysis
    
    Executes Docker container: huggingface/transformers-inference
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
    
    # Add image
    cmd.append('huggingface/transformers-inference')
    
    # Add command if specified
    
    # Execute
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True
    )
    
    return {
        'task_id': 'analyze_sentiment',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Extract Review Features",
    description="Extract Review Features - extract_review_features",
    retries=1,
    tags=["sequential", "docker"],
)
def extract_review_features() -> Dict[str, Any]:
    """
    Task: Extract Review Features
    
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
        'task_id': 'extract_review_features',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Save Enriched Reviews",
    description="Save Enriched Reviews - save_enriched_reviews",
    retries=1,
    tags=["sequential", "docker"],
)
def save_enriched_reviews() -> Dict[str, Any]:
    """
    Task: Save Enriched Reviews
    
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
        'task_id': 'save_enriched_reviews',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


# --- Main Flow ---
@flow(
    name="multilingual_product_review_analysis_pipeline",
    description="Enriches product reviews with language verification, sentiment, and key feature extraction using LLM capabilities for deeper customer insight.",
    task_runner=SequentialTaskRunner(),
    log_prints=True,
)
def multilingual_product_review_analysis_pipeline() -> Dict[str, Any]:
    """
    Main flow: Multilingual Product Review Analysis Pipeline
    
    Pattern: sequential
    Tasks: 5
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: load_and_modify_reviews")
    results['load_and_modify_reviews'] = load_and_modify_reviews()
    print(f"Executing task: detect_review_language")
    results['detect_review_language'] = detect_review_language()
    print(f"Executing task: analyze_sentiment")
    results['analyze_sentiment'] = analyze_sentiment()
    print(f"Executing task: extract_review_features")
    results['extract_review_features'] = extract_review_features()
    print(f"Executing task: save_enriched_reviews")
    results['save_enriched_reviews'] = save_enriched_reviews()
    
    return results


# --- Deployment Configuration ---
# No schedule configured - deploy manually or add schedule later
deployment = Deployment.build_from_flow(
    flow=multilingual_product_review_analysis_pipeline,
    name="multilingual_product_review_analysis_pipeline_deployment",
    work_pool_name="default-agent-pool",
    work_queue_name="default",
    tags=["sequential", "generated", "template"],
)


if __name__ == "__main__":
    # Run flow locally for testing
    multilingual_product_review_analysis_pipeline()
    
    # To deploy:
    # deployment.apply()