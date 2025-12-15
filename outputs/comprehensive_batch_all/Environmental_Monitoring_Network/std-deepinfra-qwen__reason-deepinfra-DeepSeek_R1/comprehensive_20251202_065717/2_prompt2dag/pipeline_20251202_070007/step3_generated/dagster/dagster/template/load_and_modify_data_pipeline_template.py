# ==============================================================================
# Generated Dagster Job - Sequential Pattern
# Pipeline: load_and_modify_data_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T07:11:09.452827
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
    name="reconcile_geocoding",
    description="Reconcile Geocoding",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=60,
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
    name="extend_openmeteo_data",
    description="Extend OpenMeteo Data",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=60,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def extend_openmeteo_data(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Extend OpenMeteo Data
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: extend_openmeteo_data")
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
        
        context.log.info(f"Op extend_openmeteo_data completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'extend_openmeteo_data',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op extend_openmeteo_data failed with return code {e.returncode}")
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
        context.log.error(f"Op extend_openmeteo_data timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="extend_land_use",
    description="Extend Land Use",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=60,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def extend_land_use(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Extend Land Use
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: extend_land_use")
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
        
        context.log.info(f"Op extend_land_use completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'extend_land_use',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op extend_land_use failed with return code {e.returncode}")
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
        context.log.error(f"Op extend_land_use timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="extend_population_density",
    description="Extend Population Density",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=60,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def extend_population_density(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Extend Population Density
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: extend_population_density")
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
        
        context.log.info(f"Op extend_population_density completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'extend_population_density',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op extend_population_density failed with return code {e.returncode}")
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
        context.log.error(f"Op extend_population_density timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="extend_environmental_risk",
    description="Extend Environmental Risk",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=60,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def extend_environmental_risk(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Extend Environmental Risk
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: extend_environmental_risk")
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
        
        context.log.info(f"Op extend_environmental_risk completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'extend_environmental_risk',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op extend_environmental_risk failed with return code {e.returncode}")
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
        context.log.error(f"Op extend_environmental_risk timed out")
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
    Ops: 7
    """
    # Execute ops in order based on dependencies
    
    # Op: load_and_modify_data
    # Entry point - no dependencies
    load_and_modify_data_result = load_and_modify_data()
    
    # Op: reconcile_geocoding
    # Single upstream dependency
    reconcile_geocoding_result = reconcile_geocoding()
    
    # Op: extend_openmeteo_data
    # Single upstream dependency
    extend_openmeteo_data_result = extend_openmeteo_data()
    
    # Op: extend_land_use
    # Single upstream dependency
    extend_land_use_result = extend_land_use()
    
    # Op: extend_population_density
    # Single upstream dependency
    extend_population_density_result = extend_population_density()
    
    # Op: extend_environmental_risk
    # Single upstream dependency
    extend_environmental_risk_result = extend_environmental_risk()
    
    # Op: save_final_data
    # Single upstream dependency
    save_final_data_result = save_final_data()


# --- Resources ---
from dagster import resource

@resource
def app_network_resource(context):
    """Resource: app_network"""
    # TODO: Implement resource initialization
    return {"resource_key": "app_network"}



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