# ==============================================================================
# Generated Dagster Job - Sequential Pattern
# Pipeline: file_arrival_watcher
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T13:14:29.119639
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
    name="wait_for_file",
    description="Wait for Transaction File",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=300,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def wait_for_file(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Wait for Transaction File
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: wait_for_file")
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
        
        context.log.info(f"Op wait_for_file completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'wait_for_file',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op wait_for_file failed with return code {e.returncode}")
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
        context.log.error(f"Op wait_for_file timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="validate_schema",
    description="Validate Transaction File Schema",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=300,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def validate_schema(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Validate Transaction File Schema
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: validate_schema")
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
        
        context.log.info(f"Op validate_schema completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'validate_schema',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op validate_schema failed with return code {e.returncode}")
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
        context.log.error(f"Op validate_schema timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="load_db",
    description="Load Validated Transactions to PostgreSQL",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=300,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def load_db(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Load Validated Transactions to PostgreSQL
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: load_db")
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
        
        context.log.info(f"Op load_db completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'load_db',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op load_db failed with return code {e.returncode}")
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
        context.log.error(f"Op load_db timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


# --- Job Definition ---
@job(
    name="file_arrival_watcher",
    description="Monitors daily transaction file arrivals, validates schema, and loads data into PostgreSQL.",
    executor_def=in_process_executor,
    tags={"pattern": "sequential", "generated": "template"},
)
def file_arrival_watcher():
    """
    Job: file_arrival_watcher
    
    Pattern: sequential
    Ops: 3
    """
    # Execute ops in order based on dependencies
    
    # Op: wait_for_file
    # Entry point - no dependencies
    wait_for_file_result = wait_for_file()
    
    # Op: validate_schema
    # Single upstream dependency
    validate_schema_result = validate_schema()
    
    # Op: load_db
    # Single upstream dependency
    load_db_result = load_db()


# --- Resources ---
from dagster import resource

@resource
def postgres_local_resource(context):
    """Resource: postgres_local"""
    # TODO: Implement resource initialization
    return {"resource_key": "postgres_local"}

@resource
def fs_incoming_resource(context):
    """Resource: fs_incoming"""
    # TODO: Implement resource initialization
    return {"resource_key": "fs_incoming"}



# --- Repository ---
from dagster import repository

@repository
def file_arrival_watcher_repository():
    """Repository containing file_arrival_watcher job."""
    return [file_arrival_watcher]


if __name__ == "__main__":
    # Execute job locally for testing
    result = file_arrival_watcher.execute_in_process()
    print(f"Job execution result: {result.success}")