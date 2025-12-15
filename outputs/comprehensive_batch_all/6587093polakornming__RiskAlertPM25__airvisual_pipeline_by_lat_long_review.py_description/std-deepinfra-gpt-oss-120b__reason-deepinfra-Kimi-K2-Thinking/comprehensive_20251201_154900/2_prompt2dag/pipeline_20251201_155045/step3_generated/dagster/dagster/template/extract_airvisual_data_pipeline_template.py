# ==============================================================================
# Generated Dagster Job - Sequential Pattern
# Pipeline: extract_airvisual_data_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T15:54:07.583399
# ==============================================================================

from __future__ import annotations

import os
import subprocess
from typing import Dict, Any

from dagster import (
    op,
    job,
    In,
    Out,
    Nothing,
    OpExecutionContext,
    Failure,
    RetryPolicy,
)
from dagster import in_process_executor

# --- Configuration ---
HOST_DATA_DIR = os.getenv('HOST_DATA_DIR', '/tmp/dagster/data')
CONTAINER_DATA_DIR = '/app/data'

# --- Op Definitions ---

@op(
    name="extract_airvisual_data",
    description="Extract AirVisual Data",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=300,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def extract_airvisual_data(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Extract AirVisual Data
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: extract_airvisual_data")
    context.log.info(f"Image: ")
    
    # Build docker run command
    cmd = [
        'docker', 'run',
        '--rm',
        '-v', f'{HOST_DATA_DIR}:{CONTAINER_DATA_DIR}',
    ]
    
    # Add environment variables
    
    # Add image
    cmd.append('')
    
    # Add command if specified
    
    # Execute
    context.log.info(f"Executing: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=3600,
        )
        
        context.log.info(f"Op extract_airvisual_data completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'extract_airvisual_data',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op extract_airvisual_data failed with return code {e.returncode}")
        context.log.error(f"STDERR: {e.stderr}")
        raise Failure(
            description=f"Docker container failed: {e.stderr}",
            metadata={
                'return_code': e.returncode,
                'stdout': e.stdout,
                'stderr': e.stderr,
            }
        )
    except subprocess.TimeoutExpired as e:
        context.log.error(f"Op extract_airvisual_data timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="validate_airvisual_json",
    description="Validate AirVisual JSON",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=180,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def validate_airvisual_json(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Validate AirVisual JSON
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: validate_airvisual_json")
    context.log.info(f"Image: ")
    
    # Build docker run command
    cmd = [
        'docker', 'run',
        '--rm',
        '-v', f'{HOST_DATA_DIR}:{CONTAINER_DATA_DIR}',
    ]
    
    # Add environment variables
    
    # Add image
    cmd.append('')
    
    # Add command if specified
    
    # Execute
    context.log.info(f"Executing: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=3600,
        )
        
        context.log.info(f"Op validate_airvisual_json completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'validate_airvisual_json',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op validate_airvisual_json failed with return code {e.returncode}")
        context.log.error(f"STDERR: {e.stderr}")
        raise Failure(
            description=f"Docker container failed: {e.stderr}",
            metadata={
                'return_code': e.returncode,
                'stdout': e.stdout,
                'stderr': e.stderr,
            }
        )
    except subprocess.TimeoutExpired as e:
        context.log.error(f"Op validate_airvisual_json timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="load_airvisual_to_postgresql",
    description="Load AirVisual Data to PostgreSQL",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=180,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def load_airvisual_to_postgresql(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Load AirVisual Data to PostgreSQL
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: load_airvisual_to_postgresql")
    context.log.info(f"Image: ")
    
    # Build docker run command
    cmd = [
        'docker', 'run',
        '--rm',
        '-v', f'{HOST_DATA_DIR}:{CONTAINER_DATA_DIR}',
    ]
    
    # Add environment variables
    
    # Add image
    cmd.append('')
    
    # Add command if specified
    
    # Execute
    context.log.info(f"Executing: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=3600,
        )
        
        context.log.info(f"Op load_airvisual_to_postgresql completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'load_airvisual_to_postgresql',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op load_airvisual_to_postgresql failed with return code {e.returncode}")
        context.log.error(f"STDERR: {e.stderr}")
        raise Failure(
            description=f"Docker container failed: {e.stderr}",
            metadata={
                'return_code': e.returncode,
                'stdout': e.stdout,
                'stderr': e.stderr,
            }
        )
    except subprocess.TimeoutExpired as e:
        context.log.error(f"Op load_airvisual_to_postgresql timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


# --- Job Definition ---
@job(
    name="extract_airvisual_data_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    tags={"pattern": "sequential", "generated": "template"},
)
def extract_airvisual_data_pipeline():
    """
    Job: extract_airvisual_data_pipeline
    
    Pattern: sequential
    Ops: 3
    """
    # Execute ops in order based on dependencies
    
    # Op: extract_airvisual_data
    # Entry point - no dependencies
    extract_airvisual_data_result = extract_airvisual_data()
    
    # Op: validate_airvisual_json
    # Single upstream dependency
    validate_airvisual_json_result = validate_airvisual_json()
    
    # Op: load_airvisual_to_postgresql
    # Single upstream dependency
    load_airvisual_to_postgresql_result = load_airvisual_to_postgresql()


# --- Resources ---
from dagster import resource

@resource
def airvisual_api_resource(context):
    """Resource: airvisual_api"""
    # TODO: Implement resource initialization
    return {"resource_key": "airvisual_api"}

@resource
def postgres_conn_resource(context):
    """Resource: postgres_conn"""
    # TODO: Implement resource initialization
    return {"resource_key": "postgres_conn"}



# --- Repository ---
from dagster import repository

@repository
def extract_airvisual_data_pipeline_repository():
    """Repository containing extract_airvisual_data_pipeline job."""
    return [extract_airvisual_data_pipeline]


if __name__ == "__main__":
    # Execute job locally for testing
    result = extract_airvisual_data_pipeline.execute_in_process()
    print(f"Job execution result: {result.success}")