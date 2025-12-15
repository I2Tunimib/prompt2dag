# ==============================================================================
# Generated Dagster Job - Sequential Pattern
# Pipeline: fetch_user_data_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T15:05:46.528319
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
    name="fetch_user_data",
    description="Fetch User Data",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def fetch_user_data(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Fetch User Data
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: fetch_user_data")
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
        
        context.log.info(f"Op fetch_user_data completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'fetch_user_data',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op fetch_user_data failed with return code {e.returncode}")
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
        context.log.error(f"Op fetch_user_data timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="process_user_data",
    description="Process User Data",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def process_user_data(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Process User Data
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: process_user_data")
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
        
        context.log.info(f"Op process_user_data completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'process_user_data',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op process_user_data failed with return code {e.returncode}")
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
        context.log.error(f"Op process_user_data timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="create_user_table",
    description="Create User Table",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def create_user_table(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Create User Table
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: create_user_table")
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
        
        context.log.info(f"Op create_user_table completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'create_user_table',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op create_user_table failed with return code {e.returncode}")
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
        context.log.error(f"Op create_user_table timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="insert_user_data",
    description="Insert User Data",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def insert_user_data(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Insert User Data
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: insert_user_data")
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
        
        context.log.info(f"Op insert_user_data completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'insert_user_data',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op insert_user_data failed with return code {e.returncode}")
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
        context.log.error(f"Op insert_user_data timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


# --- Job Definition ---
@job(
    name="fetch_user_data_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    tags={"pattern": "sequential", "generated": "template"},
)
def fetch_user_data_pipeline():
    """
    Job: fetch_user_data_pipeline
    
    Pattern: sequential
    Ops: 4
    """
    # Execute ops in order based on dependencies
    
    # Op: fetch_user_data
    # Entry point - no dependencies
    fetch_user_data_result = fetch_user_data()
    
    # Op: process_user_data
    # Single upstream dependency
    process_user_data_result = process_user_data()
    
    # Op: create_user_table
    # Single upstream dependency
    create_user_table_result = create_user_table()
    
    # Op: insert_user_data
    # Single upstream dependency
    insert_user_data_result = insert_user_data()


# --- Resources ---
from dagster import resource

@resource
def postgres_resource(context):
    """Resource: postgres"""
    # TODO: Implement resource initialization
    return {"resource_key": "postgres"}

@resource
def reqres_resource(context):
    """Resource: reqres"""
    # TODO: Implement resource initialization
    return {"resource_key": "reqres"}



# --- Repository ---
from dagster import repository

@repository
def fetch_user_data_pipeline_repository():
    """Repository containing fetch_user_data_pipeline job."""
    return [fetch_user_data_pipeline]


if __name__ == "__main__":
    # Execute job locally for testing
    result = fetch_user_data_pipeline.execute_in_process()
    print(f"Job execution result: {result.success}")