# ==============================================================================
# Generated Prefect Flow - Sequential Pattern
# Pipeline: load_and_modify_data_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T04:53:46.886413
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
    cmd.extend(['-e', 'DATE_COLUMN=submission_date'])
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
    name="Language Detection",
    description="Language Detection - language_detection",
    retries=1,
    tags=["sequential", "docker"],
)
def language_detection() -> Dict[str, Any]:
    """
    Task: Language Detection
    
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
    cmd.extend(['-e', 'TEXT_COLUMN=review_text'])
    cmd.extend(['-e', 'LANG_CODE_COLUMN=language_code'])
    cmd.extend(['-e', 'OUTPUT_FILE=lang_detected_2.json'])
    
    # Add image
    cmd.append('jmockit/language-detection')
    
    # Add command if specified
    cmd.extend(["python", "detect_language.py"])
    
    # Execute
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True
    )
    
    return {
        'task_id': 'language_detection',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Sentiment Analysis",
    description="Sentiment Analysis - sentiment_analysis",
    retries=1,
    tags=["sequential", "docker"],
)
def sentiment_analysis() -> Dict[str, Any]:
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
    cmd.extend(['-e', 'MODEL_NAME=distilbert-base-uncased-finetuned-sst-2-english'])
    cmd.extend(['-e', 'TEXT_COLUMN=review_text'])
    cmd.extend(['-e', 'OUTPUT_COLUMN=sentiment_score'])
    
    # Add image
    cmd.append('huggingface/transformers-inference')
    
    # Add command if specified
    cmd.extend(["python", "sentiment_analysis.py"])
    
    # Execute
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True
    )
    
    return {
        'task_id': 'sentiment_analysis',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Category Extraction",
    description="Category Extraction - category_extraction",
    retries=1,
    tags=["sequential", "docker"],
)
def category_extraction() -> Dict[str, Any]:
    """
    Task: Category Extraction
    
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
    cmd.extend(['-e', 'EXTENDER_ID=featureExtractor'])
    cmd.extend(['-e', 'TEXT_COLUMN=review_text'])
    cmd.extend(['-e', 'OUTPUT_COLUMN=mentioned_features'])
    
    # Add image
    cmd.append('i2t-backendwithintertwino6-column-extension:latest')
    
    # Add command if specified
    cmd.extend(["python", "category_extraction.py"])
    
    # Execute
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True
    )
    
    return {
        'task_id': 'category_extraction',
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


@task(
    name="Save Final Data",
    description="Save Final Data - save_final_data",
    retries=1,
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
    cmd.extend(["python", "save_data.py"])
    
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
    Tasks: 5
    """
    results = {}
    
    # Execute tasks in order
    print(f"Executing task: load_and_modify_data")
    results['load_and_modify_data'] = load_and_modify_data()
    print(f"Executing task: language_detection")
    results['language_detection'] = language_detection()
    print(f"Executing task: sentiment_analysis")
    results['sentiment_analysis'] = sentiment_analysis()
    print(f"Executing task: category_extraction")
    results['category_extraction'] = category_extraction()
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