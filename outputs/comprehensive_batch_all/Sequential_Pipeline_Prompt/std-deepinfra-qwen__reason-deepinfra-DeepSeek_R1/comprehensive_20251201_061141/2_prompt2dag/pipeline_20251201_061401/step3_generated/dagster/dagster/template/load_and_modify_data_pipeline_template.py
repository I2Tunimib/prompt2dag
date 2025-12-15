# ==============================================================================
# Generated Dagster Job - Sequential Pattern
# Pipeline: load_and_modify_data_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T06:22:09.680587
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
from dagster_docker import docker_executor

# --- Configuration ---
HOST_DATA_DIR = os.getenv('HOST_DATA_DIR', '/tmp/dagster/data')
CONTAINER_DATA_DIR = '/app/data'

# --- Op Definitions ---

@op(
    name="load_and_modify_data",
    description="Load and Modify Data",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=60,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def load_and_modify_data(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Load and Modify Data
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: load_and_modify_data")
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
        
        context.log.info(f"Op load_and_modify_data completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'load_and_modify_data',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op load_and_modify_data failed with return code {e.returncode}")
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
        context.log.error(f"Op load_and_modify_data timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="reconcile_data",
    description="Data Reconciliation",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=60,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def reconcile_data(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Data Reconciliation
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: reconcile_data")
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
        
        context.log.info(f"Op reconcile_data completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'reconcile_data',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op reconcile_data failed with return code {e.returncode}")
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
        context.log.error(f"Op reconcile_data timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="extend_with_weather_data",
    description="OpenMeteo Data Extension",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=60,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def extend_with_weather_data(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: OpenMeteo Data Extension
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: extend_with_weather_data")
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
        
        context.log.info(f"Op extend_with_weather_data completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'extend_with_weather_data',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op extend_with_weather_data failed with return code {e.returncode}")
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
        context.log.error(f"Op extend_with_weather_data timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="extend_columns",
    description="Column Extension",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=60,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def extend_columns(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Column Extension
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: extend_columns")
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
        
        context.log.info(f"Op extend_columns completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'extend_columns',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op extend_columns failed with return code {e.returncode}")
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
        context.log.error(f"Op extend_columns timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="save_final_data",
    description="Save Final Data",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=60,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def save_final_data(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Save Final Data
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: save_final_data")
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
        
        context.log.info(f"Op save_final_data completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'save_final_data',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op save_final_data failed with return code {e.returncode}")
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
        context.log.error(f"Op save_final_data timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


# --- Job Definition ---
@job(
    name="load_and_modify_data_pipeline",
    description="No description provided.",
    executor_def=docker_executor,
    tags={"pattern": "sequential", "generated": "template"},
)
def load_and_modify_data_pipeline():
    """
    Job: load_and_modify_data_pipeline
    
    Pattern: sequential
    Ops: 5
    """
    # Execute ops in order based on dependencies
    
    # Op: load_and_modify_data
    # Entry point - no dependencies
    load_and_modify_data_result = load_and_modify_data()
    
    # Op: reconcile_data
    # Single upstream dependency
    reconcile_data_result = reconcile_data()
    
    # Op: extend_with_weather_data
    # Single upstream dependency
    extend_with_weather_data_result = extend_with_weather_data()
    
    # Op: extend_columns
    # Single upstream dependency
    extend_columns_result = extend_columns()
    
    # Op: save_final_data
    # Single upstream dependency
    save_final_data_result = save_final_data()


# --- Resources ---
from dagster import resource

@resource
def openmeteo_api_resource(context):
    """Resource: openmeteo_api"""
    # TODO: Implement resource initialization
    return {"resource_key": "openmeteo_api"}

@resource
def geocoding_api_resource(context):
    """Resource: geocoding_api"""
    # TODO: Implement resource initialization
    return {"resource_key": "geocoding_api"}

@resource
def data_directory_resource(context):
    """Resource: data_directory"""
    # TODO: Implement resource initialization
    return {"resource_key": "data_directory"}



# --- Repository ---
from dagster import repository

@repository
def load_and_modify_data_pipeline_repository():
    """Repository containing load_and_modify_data_pipeline job."""
    return [load_and_modify_data_pipeline]


if __name__ == "__main__":
    # Execute job locally for testing
    result = load_and_modify_data_pipeline.execute_in_process()
    print(f"Job execution result: {result.success}")