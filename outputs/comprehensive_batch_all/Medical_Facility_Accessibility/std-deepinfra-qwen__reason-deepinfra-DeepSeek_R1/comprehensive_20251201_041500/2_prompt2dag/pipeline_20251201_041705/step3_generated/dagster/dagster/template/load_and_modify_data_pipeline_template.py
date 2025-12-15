# ==============================================================================
# Generated Dagster Job - Sequential Pattern
# Pipeline: load_and_modify_data_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T04:26:09.975839
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
        delay=0,
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
    name="reconcile_geocoding",
    description="Reconcile Geocoding",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=0,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def reconcile_geocoding(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Reconcile Geocoding
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: reconcile_geocoding")
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
        
        context.log.info(f"Op reconcile_geocoding completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'reconcile_geocoding',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op reconcile_geocoding failed with return code {e.returncode}")
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
        context.log.error(f"Op reconcile_geocoding timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="calculate_distance_pt",
    description="Calculate Distance to Public Transport",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=0,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def calculate_distance_pt(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Calculate Distance to Public Transport
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: calculate_distance_pt")
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
        
        context.log.info(f"Op calculate_distance_pt completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'calculate_distance_pt',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op calculate_distance_pt failed with return code {e.returncode}")
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
        context.log.error(f"Op calculate_distance_pt timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="calculate_distance_residential",
    description="Calculate Distance to Residential Areas",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=0,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def calculate_distance_residential(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Calculate Distance to Residential Areas
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: calculate_distance_residential")
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
        
        context.log.info(f"Op calculate_distance_residential completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'calculate_distance_residential',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op calculate_distance_residential failed with return code {e.returncode}")
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
        context.log.error(f"Op calculate_distance_residential timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="save_final_data",
    description="Save Final Data",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=0,
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
    
    # Op: reconcile_geocoding
    # Single upstream dependency
    reconcile_geocoding_result = reconcile_geocoding()
    
    # Op: calculate_distance_pt
    # Single upstream dependency
    calculate_distance_pt_result = calculate_distance_pt()
    
    # Op: calculate_distance_residential
    # Single upstream dependency
    calculate_distance_residential_result = calculate_distance_residential()
    
    # Op: save_final_data
    # Single upstream dependency
    save_final_data_result = save_final_data()


# --- Resources ---
from dagster import resource

@resource
def data_dir_resource(context):
    """Resource: data_dir"""
    # TODO: Implement resource initialization
    return {"resource_key": "data_dir"}

@resource
def here_api_resource(context):
    """Resource: here_api"""
    # TODO: Implement resource initialization
    return {"resource_key": "here_api"}



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