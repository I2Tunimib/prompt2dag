# ==============================================================================
# Generated Dagster Job - Sequential Pattern
# Pipeline: get_airvisual_data_hourly_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T15:44:43.630581
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
    name="get_airvisual_data_hourly",
    description="Fetch AirVisual Data",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=300,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def get_airvisual_data_hourly(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Fetch AirVisual Data
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: get_airvisual_data_hourly")
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
        
        context.log.info(f"Op get_airvisual_data_hourly completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'get_airvisual_data_hourly',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op get_airvisual_data_hourly failed with return code {e.returncode}")
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
        context.log.error(f"Op get_airvisual_data_hourly timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="read_data_airvisual",
    description="Read and Validate AirVisual Data",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=180,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def read_data_airvisual(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Read and Validate AirVisual Data
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: read_data_airvisual")
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
        
        context.log.info(f"Op read_data_airvisual completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'read_data_airvisual',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op read_data_airvisual failed with return code {e.returncode}")
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
        context.log.error(f"Op read_data_airvisual timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="load_data_airvisual_to_postgresql",
    description="Load AirVisual Data to PostgreSQL",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=180,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def load_data_airvisual_to_postgresql(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Load AirVisual Data to PostgreSQL
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: load_data_airvisual_to_postgresql")
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
        
        context.log.info(f"Op load_data_airvisual_to_postgresql completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'load_data_airvisual_to_postgresql',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op load_data_airvisual_to_postgresql failed with return code {e.returncode}")
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
        context.log.error(f"Op load_data_airvisual_to_postgresql timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


# --- Job Definition ---
@job(
    name="get_airvisual_data_hourly_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    tags={"pattern": "sequential", "generated": "template"},
)
def get_airvisual_data_hourly_pipeline():
    """
    Job: get_airvisual_data_hourly_pipeline
    
    Pattern: sequential
    Ops: 3
    """
    # Execute ops in order based on dependencies
    
    # Op: get_airvisual_data_hourly
    # Entry point - no dependencies
    get_airvisual_data_hourly_result = get_airvisual_data_hourly()
    
    # Op: read_data_airvisual
    # Single upstream dependency
    read_data_airvisual_result = read_data_airvisual()
    
    # Op: load_data_airvisual_to_postgresql
    # Single upstream dependency
    load_data_airvisual_to_postgresql_result = load_data_airvisual_to_postgresql()


# --- Resources ---
from dagster import resource

@resource
def local_filesystem_resource(context):
    """Resource: local_filesystem"""
    # TODO: Implement resource initialization
    return {"resource_key": "local_filesystem"}

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
def get_airvisual_data_hourly_pipeline_repository():
    """Repository containing get_airvisual_data_hourly_pipeline job."""
    return [get_airvisual_data_hourly_pipeline]


if __name__ == "__main__":
    # Execute job locally for testing
    result = get_airvisual_data_hourly_pipeline.execute_in_process()
    print(f"Job execution result: {result.success}")