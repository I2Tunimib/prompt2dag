# ==============================================================================
# Generated Dagster Job - Sequential Pattern
# Pipeline: load_modify_facilities_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T10:48:05.395632
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
    name="load_modify_facilities",
    description="Load and Modify Facilities Data",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=60,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def load_modify_facilities(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Load and Modify Facilities Data
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: load_modify_facilities")
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
        
        context.log.info(f"Op load_modify_facilities completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'load_modify_facilities',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op load_modify_facilities failed with return code {e.returncode}")
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
        context.log.error(f"Op load_modify_facilities timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="geocode_facilities",
    description="Geocode Facilities Using HERE API",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=60,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def geocode_facilities(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Geocode Facilities Using HERE API
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: geocode_facilities")
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
        
        context.log.info(f"Op geocode_facilities completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'geocode_facilities',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op geocode_facilities failed with return code {e.returncode}")
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
        context.log.error(f"Op geocode_facilities timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="calculate_distance_to_public_transport",
    description="Calculate Distance to Nearest Public Transport",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=60,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def calculate_distance_to_public_transport(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Calculate Distance to Nearest Public Transport
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: calculate_distance_to_public_transport")
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
        
        context.log.info(f"Op calculate_distance_to_public_transport completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'calculate_distance_to_public_transport',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op calculate_distance_to_public_transport failed with return code {e.returncode}")
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
        context.log.error(f"Op calculate_distance_to_public_transport timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="calculate_distance_to_residential_areas",
    description="Calculate Distance to Nearest Residential Area",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=60,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def calculate_distance_to_residential_areas(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Calculate Distance to Nearest Residential Area
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: calculate_distance_to_residential_areas")
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
        
        context.log.info(f"Op calculate_distance_to_residential_areas completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'calculate_distance_to_residential_areas',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op calculate_distance_to_residential_areas failed with return code {e.returncode}")
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
        context.log.error(f"Op calculate_distance_to_residential_areas timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="save_facilities_accessibility",
    description="Save Enriched Facility Accessibility Data",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=60,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def save_facilities_accessibility(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Save Enriched Facility Accessibility Data
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: save_facilities_accessibility")
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
        
        context.log.info(f"Op save_facilities_accessibility completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'save_facilities_accessibility',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op save_facilities_accessibility failed with return code {e.returncode}")
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
        context.log.error(f"Op save_facilities_accessibility timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


# --- Job Definition ---
@job(
    name="load_modify_facilities_pipeline",
    description="No description provided.",
    executor_def=docker_executor,
    tags={"pattern": "sequential", "generated": "template"},
)
def load_modify_facilities_pipeline():
    """
    Job: load_modify_facilities_pipeline
    
    Pattern: sequential
    Ops: 5
    """
    # Execute ops in order based on dependencies
    
    # Op: load_modify_facilities
    # Entry point - no dependencies
    load_modify_facilities_result = load_modify_facilities()
    
    # Op: geocode_facilities
    # Single upstream dependency
    geocode_facilities_result = geocode_facilities()
    
    # Op: calculate_distance_to_public_transport
    # Single upstream dependency
    calculate_distance_to_public_transport_result = calculate_distance_to_public_transport()
    
    # Op: calculate_distance_to_residential_areas
    # Single upstream dependency
    calculate_distance_to_residential_areas_result = calculate_distance_to_residential_areas()
    
    # Op: save_facilities_accessibility
    # Single upstream dependency
    save_facilities_accessibility_result = save_facilities_accessibility()


# --- Resources ---
from dagster import resource

@resource
def data_dir_fs_resource(context):
    """Resource: data_dir_fs"""
    # TODO: Implement resource initialization
    return {"resource_key": "data_dir_fs"}



# --- Repository ---
from dagster import repository

@repository
def load_modify_facilities_pipeline_repository():
    """Repository containing load_modify_facilities_pipeline job."""
    return [load_modify_facilities_pipeline]


if __name__ == "__main__":
    # Execute job locally for testing
    result = load_modify_facilities_pipeline.execute_in_process()
    print(f"Job execution result: {result.success}")