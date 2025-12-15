# ==============================================================================
# Generated Dagster Job - Sequential Pattern
# Pipeline: create_customer_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T13:27:59.788605
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
    name="create_customer",
    description="Create Customer",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=300,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def create_customer(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Create Customer
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: create_customer")
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
        
        context.log.info(f"Op create_customer completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'create_customer',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op create_customer failed with return code {e.returncode}")
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
        context.log.error(f"Op create_customer timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="generate_customer_token",
    description="Generate Customer Token",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=300,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def generate_customer_token(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Generate Customer Token
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: generate_customer_token")
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
        
        context.log.info(f"Op generate_customer_token completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'generate_customer_token',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op generate_customer_token failed with return code {e.returncode}")
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
        context.log.error(f"Op generate_customer_token timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="get_customer_info",
    description="Get Customer Info",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=300,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def get_customer_info(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Get Customer Info
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: get_customer_info")
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
        
        context.log.info(f"Op get_customer_info completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'get_customer_info',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op get_customer_info failed with return code {e.returncode}")
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
        context.log.error(f"Op get_customer_info timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


# --- Job Definition ---
@job(
    name="create_customer_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    tags={"pattern": "sequential", "generated": "template"},
)
def create_customer_pipeline():
    """
    Job: create_customer_pipeline
    
    Pattern: sequential
    Ops: 3
    """
    # Execute ops in order based on dependencies
    
    # Op: create_customer
    # Entry point - no dependencies
    create_customer_result = create_customer()
    
    # Op: generate_customer_token
    # Single upstream dependency
    generate_customer_token_result = generate_customer_token()
    
    # Op: get_customer_info
    # Single upstream dependency
    get_customer_info_result = get_customer_info()


# --- Resources ---
from dagster import resource

@resource
def magento_default_resource(context):
    """Resource: magento_default"""
    # TODO: Implement resource initialization
    return {"resource_key": "magento_default"}



# --- Repository ---
from dagster import repository

@repository
def create_customer_pipeline_repository():
    """Repository containing create_customer_pipeline job."""
    return [create_customer_pipeline]


if __name__ == "__main__":
    # Execute job locally for testing
    result = create_customer_pipeline.execute_in_process()
    print(f"Job execution result: {result.success}")