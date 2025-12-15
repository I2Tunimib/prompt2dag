# ==============================================================================
# Generated Dagster Job - Sequential Pattern
# Pipeline: Data Transformation Pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T09:03:03.513162
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
    name="initialize_pipeline",
    description="Initialize Pipeline",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def initialize_pipeline(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Initialize Pipeline
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: initialize_pipeline")
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
        
        context.log.info(f"Op initialize_pipeline completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'initialize_pipeline',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op initialize_pipeline failed with return code {e.returncode}")
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
        context.log.error(f"Op initialize_pipeline timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="parse_input_params",
    description="Parse Input Parameters",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def parse_input_params(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Parse Input Parameters
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: parse_input_params")
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
        
        context.log.info(f"Op parse_input_params completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'parse_input_params',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op parse_input_params failed with return code {e.returncode}")
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
        context.log.error(f"Op parse_input_params timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="create_compilation_result",
    description="Create Dataform Compilation Result",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def create_compilation_result(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Create Dataform Compilation Result
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: create_compilation_result")
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
        
        context.log.info(f"Op create_compilation_result completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'create_compilation_result',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op create_compilation_result failed with return code {e.returncode}")
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
        context.log.error(f"Op create_compilation_result timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="create_workflow_invocation",
    description="Create Dataform Workflow Invocation",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def create_workflow_invocation(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Create Dataform Workflow Invocation
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: create_workflow_invocation")
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
        
        context.log.info(f"Op create_workflow_invocation completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'create_workflow_invocation',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op create_workflow_invocation failed with return code {e.returncode}")
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
        context.log.error(f"Op create_workflow_invocation timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="monitor_workflow_state",
    description="Monitor Workflow Invocation State",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def monitor_workflow_state(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Monitor Workflow Invocation State
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: monitor_workflow_state")
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
        
        context.log.info(f"Op monitor_workflow_state completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'monitor_workflow_state',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op monitor_workflow_state failed with return code {e.returncode}")
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
        context.log.error(f"Op monitor_workflow_state timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="finalize_pipeline",
    description="Finalize Pipeline",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def finalize_pipeline(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Finalize Pipeline
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: finalize_pipeline")
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
        
        context.log.info(f"Op finalize_pipeline completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'finalize_pipeline',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op finalize_pipeline failed with return code {e.returncode}")
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
        context.log.error(f"Op finalize_pipeline timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


# --- Job Definition ---
@job(
    name="data_transformation_pipeline",
    description="Comprehensive Pipeline Description",
    executor_def=in_process_executor,
    tags={"pattern": "sequential", "generated": "template"},
)
def data_transformation_pipeline():
    """
    Job: Data Transformation Pipeline
    
    Pattern: sequential
    Ops: 6
    """
    # Execute ops in order based on dependencies
    
    # Op: initialize_pipeline
    # Entry point - no dependencies
    initialize_pipeline_result = initialize_pipeline()
    
    # Op: parse_input_params
    # Single upstream dependency
    parse_input_params_result = parse_input_params()
    
    # Op: create_compilation_result
    # Single upstream dependency
    create_compilation_result_result = create_compilation_result()
    
    # Op: create_workflow_invocation
    # Single upstream dependency
    create_workflow_invocation_result = create_workflow_invocation()
    
    # Op: monitor_workflow_state
    # Single upstream dependency
    monitor_workflow_state_result = monitor_workflow_state()
    
    # Op: finalize_pipeline
    # Single upstream dependency
    finalize_pipeline_result = finalize_pipeline()


# --- Resources ---
from dagster import resource

@resource
def modelling_cloud_default_resource(context):
    """Resource: modelling_cloud_default"""
    # TODO: Implement resource initialization
    return {"resource_key": "modelling_cloud_default"}



# --- Repository ---
from dagster import repository

@repository
def data_transformation_pipeline_repository():
    """Repository containing data_transformation_pipeline job."""
    return [data_transformation_pipeline]


if __name__ == "__main__":
    # Execute job locally for testing
    result = data_transformation_pipeline.execute_in_process()
    print(f"Job execution result: {result.success}")