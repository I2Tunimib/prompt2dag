# ==============================================================================
# Generated Dagster Job - Sequential Pattern
# Pipeline: load_and_modify_data_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T12:50:52.057268
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
        delay=30,
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
    name="data_reconciliation",
    description="Data Reconciliation",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=30,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def data_reconciliation(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Data Reconciliation
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: data_reconciliation")
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
        
        context.log.info(f"Op data_reconciliation completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'data_reconciliation',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op data_reconciliation failed with return code {e.returncode}")
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
        context.log.error(f"Op data_reconciliation timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="openmeteo_data_extension",
    description="OpenMeteo Data Extension",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=30,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def openmeteo_data_extension(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: OpenMeteo Data Extension
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: openmeteo_data_extension")
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
        
        context.log.info(f"Op openmeteo_data_extension completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'openmeteo_data_extension',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op openmeteo_data_extension failed with return code {e.returncode}")
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
        context.log.error(f"Op openmeteo_data_extension timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="column_extension",
    description="Column Extension",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=30,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def column_extension(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Column Extension
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: column_extension")
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
        
        context.log.info(f"Op column_extension completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'column_extension',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op column_extension failed with return code {e.returncode}")
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
        context.log.error(f"Op column_extension timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="save_final_data",
    description="Save Final Data",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=30,
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
    
    # Op: data_reconciliation
    # Single upstream dependency
    data_reconciliation_result = data_reconciliation()
    
    # Op: openmeteo_data_extension
    # Single upstream dependency
    openmeteo_data_extension_result = openmeteo_data_extension()
    
    # Op: column_extension
    # Single upstream dependency
    column_extension_result = column_extension()
    
    # Op: save_final_data
    # Single upstream dependency
    save_final_data_result = save_final_data()


# --- Resources ---
from dagster import resource

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

@resource
def openmeteo_api_resource(context):
    """Resource: openmeteo_api"""
    # TODO: Implement resource initialization
    return {"resource_key": "openmeteo_api"}



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